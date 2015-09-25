/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.data.{Range => aRange}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.data.tables.{GeoMesaTable, RecordTable}
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.accumulo.index.{AttributeIdxStrategy, QueryPlanner, QueryStrategyDecider}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

/**
 * NB:  This test uses MockAccumulo, which is being deprecated:
 * https://issues.apache.org/jira/browse/ACCUMULO-3920
 *
 * TODO:  We ought to consider rewriting this (and other dependent) tests
 * to use MiniAccumulo or some other test harness.
 */

@RunWith(classOf[JUnitRunner])
class AccumuloFeatureWriterTest extends Specification with TestWithDataStore {
  sequential

  import TestType._
  var test: TestType = TestPoint

  override def spec: String = getTestSpec(test)

  val sdf = new SimpleDateFormat("yyyyMMdd")
  sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))

  val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'")
  sdf2.setTimeZone(TimeZone.getTimeZone("Zulu"))

  def queryPlanner = new QueryPlanner(sft, ds.getFeatureEncoding(sft), ds.getIndexSchemaFmt(sftName), ds,
    ds.strategyHints(sft))

  def GeospatialStrategyOpt =
    if (spec.matches("(?i).*geom:Point:.*")) None // Some(StrategyType.Z3)
    else Some(StrategyType.ST)
  def AgeStrategyOpt =
    if (spec.matches("(?i).*age:Integer:index=true.*")) Some(StrategyType.ATTRIBUTE)
    else None
  def NameStrategyOpt =
    if (spec.matches("(?i).*name:String:index=true.*")) Some(StrategyType.ATTRIBUTE)
    else None
  def TemporalStrategyOpt =
    if (spec.matches("(?i).*dtg:Date:index=true.*")) Some(StrategyType.ATTRIBUTE)
    else None
  def IdStrategyOpt = Some(StrategyType.RECORD)

  def dateToIndex(offset: Int = 0) =
    sdf.parse(f"201401${2 + offset}%02d")
  
  def geomToIndex(offset: Int = 0): Geometry = 
    WKTUtils.read(s"POINT(${offset + 45.0} ${offset + 49.0})")

  val BaseData = Seq(("will", 56), ("george", 33), ("sue", 99), ("karen", 50), ("bob", 56))

  def generateFeatures(reuseLocation: Boolean, reuseDTG: Boolean): Seq[SimpleFeature] =
    BaseData.zipWithIndex.map {
      case ((name, age), index) =>
        val dtg = dateToIndex(if (reuseDTG) 0 else index)
        val geom = geomToIndex(if (reuseLocation) 0 else index)
        val id = s"fid${1 + index}"
        AvroSimpleFeatureFactory.buildAvroFeature(sft, Seq(name, age.asInstanceOf[AnyRef], dtg, geom), id)
    }

  def featureSeq = generateFeatures(reuseLocation = false, reuseDTG = false)

  def featureCollection = {
    val coll = new DefaultFeatureCollection
    featureSeq.foreach(feature => coll.add(feature))
    coll
  }

  "dummy" should {
    "delete this" >> {
      val cn = getClass.getSimpleName
      println(s"\n\n[$cn STRATEGIES]\n\tt:  $TemporalStrategyOpt\n\ta:  $AgeStrategyOpt\n\tn:  $NameStrategyOpt\n\tg:  $GeospatialStrategyOpt\n")
      1 must equalTo(1)
    }
  }

  val lock = new Object()


  def deleteOneRecordSafely = {
    // remove all data
    clearTablesHard()

    /* add some features */
    addFeatures(featureSeq)

    /* turn will into billy */
    val filter = CQL.toFilter("name = 'will'")
    fs.modifyFeatures(Array("name", "age"), Array("billy", 25.asInstanceOf[AnyRef]), filter)

    /* delete george */
    val deleteFilter = CQL.toFilter("name = 'george'")
    fs.removeFeatures(deleteFilter)

    def queryViaPlanner(cqlFilter: Filter, strategyOpt: Option[StrategyType] = None) =
      queryPlanner.runQuery(new Query(sftName, cqlFilter), strategyOpt).toList

    def queryViaStore(cqlFilter: Filter) =
      fs.getFeatures(new Query(sftName, cqlFilter)).features().toList

    def doQuery(cqlFilter: Filter, shouldBeEmpty: Boolean = false, strategyOpt: Option[StrategyType] = None) = {
      println(s"[${getClass.getSimpleName} QUERY${if (shouldBeEmpty) " EMPTY" else ""} $strategyOpt] filter:  $cqlFilter")

      /* Let's read out what we wrote... */
      val features = queryViaStore(cqlFilter)

      // ensure that explicitly setting the strategy doesn't change the size of the results set
      if (strategyOpt.isDefined)
        features.size must equalTo(queryViaPlanner(cqlFilter, strategyOpt).size)

      if (shouldBeEmpty) {
        features must haveSize(0)
      } else {
        features must haveSize(4)
        features.map(f => (f.getAttribute("name"), f.getAttribute("age"))) must
          containTheSameElementsAs(Seq(("billy", 25), ("sue", 99), ("karen", 50), ("bob", 56)))
        features.map(f => (f.getAttribute("name"), f.getID)) must
          containTheSameElementsAs(Seq(("billy", "fid1"), ("sue", "fid3"), ("karen", "fid4"), ("bob", "fid5")))
      }
    }

    val george = featureSeq.filter(_.getAttribute("name").toString == "george").head
    val envGeorge = george.getDefaultGeometry.asInstanceOf[Geometry].getEnvelopeInternal
    val bboxGeorge = "BBOX(geom, " +
      s"${envGeorge.getMinX - 0.001}, ${envGeorge.getMinY - 0.001}," +
      s"${envGeorge.getMaxX + 0.001}, ${envGeorge.getMaxY + 0.001})"
    val allIDs = featureSeq.map(_.getID)
    val dtGeorge = george.getAttribute("dtg").asInstanceOf[Date]
    val dtsGeorge = sdf2.format(dtGeorge)
    val dtsGeorgePre = sdf2.format(new Date(dtGeorge.getTime - 1L))
    val dtsGeorgePost = sdf2.format(new Date(dtGeorge.getTime + 1L))

    // these queries should continue to return only non-George data...

    /* query everything */
    doQuery(Filter.INCLUDE, shouldBeEmpty = false, None)

    /* query by BBOX */
    doQuery(ECQL.toFilter("BBOX(geom, 40, 40, 80, 80)"), shouldBeEmpty = false, GeospatialStrategyOpt)

    /* query by TIME */
    doQuery(ECQL.toFilter(s"dtg DURING 2000-01-01T00:00:00.0Z/2020-12-31T23:59:59.9Z"), shouldBeEmpty = false, TemporalStrategyOpt)

    /* query by AGE */
    doQuery(ECQL.toFilter(s"age BETWEEN 20 AND 100"), shouldBeEmpty = false, AgeStrategyOpt)

    /* query by NAME */
    doQuery(ECQL.toFilter(s"(name < 'george') OR (name > 'george')"), shouldBeEmpty = false, NameStrategyOpt)

    /* query by ID */
    doQuery(ECQL.toFilter(s"IN (${allIDs.mkString("'","', '", "'")})"), shouldBeEmpty = false, IdStrategyOpt)

    // these queries should no longer return any data...

    /* query by BBOX */
    doQuery(ECQL.toFilter(bboxGeorge), shouldBeEmpty = true, GeospatialStrategyOpt)

    /* query by TIME */
    doQuery(ECQL.toFilter(s"(dtg DURING $dtsGeorgePre/$dtsGeorgePost)"), shouldBeEmpty = true, TemporalStrategyOpt)

    /* query by AGE */
    doQuery(ECQL.toFilter(s"age BETWEEN 32 AND 34"), shouldBeEmpty = true, AgeStrategyOpt)

    /* query by NAME */
    doQuery(ECQL.toFilter(s"name='george'"), shouldBeEmpty = true, NameStrategyOpt)

    /* query by ID */
    doQuery(ECQL.toFilter(s"IN ('${george.getID}')"), shouldBeEmpty = true, IdStrategyOpt)
  }

  def repopulateTable = {
    /* repopulate it */
    val writer = ds.getFeatureWriter(sftName, Transaction.AUTO_COMMIT)
    featureCollection.foreach {f =>
      val writerCreatedFeature = writer.next()
      writerCreatedFeature.setAttributes(f.getAttributes)
      writerCreatedFeature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      writerCreatedFeature.getUserData.put(Hints.PROVIDED_FID, f.getID)
      writer.write()
    }
    writer.close()

    val features = fs.getFeatures(Filter.INCLUDE).features().toSeq

    features must haveSize(5)

    features.map(f => (f.getAttribute("name"), f.getID)) must
      containTheSameElementsAs(Seq(("will", "fid1"), ("george", "fid2"), ("sue", "fid3"), ("karen", "fid4"), ("bob", "fid5")))
  }

  def deleteAndRepopulate() = {
    deleteOneRecordSafely
    repopulateTable
  }

  def updateViaECQL = {
    clearTablesHard()

    addFeatures(featureSeq)

    val filter = CQL.toFilter("(age > 50 AND age < 99) or (name = 'karen')")
    fs.modifyFeatures(Array("age"), Array(60.asInstanceOf[AnyRef]), filter)

    val updated = fs.getFeatures(ECQL.toFilter("age = 60")).features.toSeq

    updated.map(f => (f.getAttribute("name"), f.getAttribute("age"))) must
      containTheSameElementsAs(Seq(("will", 60), ("karen", 60), ("bob", 60)))
    updated.map(f => (f.getAttribute("name"), f.getID)) must
      containTheSameElementsAs(Seq(("will", "fid1"), ("karen", "fid4"), ("bob", "fid5")))
  }

  def removeFeatures = {
    clearTablesHard()

    addFeatures(featureSeq)

    val writer = ds.getFeatureWriter(sftName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
    while (writer.hasNext) {
      writer.next()
      writer.remove()
    }
    writer.close()

    val features = fs.getFeatures(Filter.INCLUDE).features().toSeq
    features must beEmpty

    forall(GeoMesaTable.getTableNames(sft, ds)) { name =>
      val scanner = connector.createScanner(name, new Authorizations())
      try {
        scanner.iterator().hasNext must beFalse
      } finally {
        scanner.close()
      }
    }
  }

  def addInsideTransaction = {
    clearTablesHard()

    val c = new DefaultFeatureCollection
    c.add(AvroSimpleFeatureFactory.buildAvroFeature(sft, Array("dude1", 15.asInstanceOf[AnyRef], null, geomToIndex()), "fid10"))
    c.add(AvroSimpleFeatureFactory.buildAvroFeature(sft, Array("dude2", 16.asInstanceOf[AnyRef], null, geomToIndex()), "fid11"))
    c.add(AvroSimpleFeatureFactory.buildAvroFeature(sft, Array("dude3", 17.asInstanceOf[AnyRef], null, geomToIndex()), "fid12"))

    val trans = new DefaultTransaction("trans1")
    fs.setTransaction(trans)
    try {
      fs.addFeatures(c)
      trans.commit()

      val features = fs.getFeatures(ECQL.toFilter("(age = 15) or (age = 16) or (age = 17)")).features().toSeq
      features.map(f => (f.getAttribute("name"), f.getAttribute("age"))) must
        containTheSameElementsAs(Seq(("dude1", 15), ("dude2", 16), ("dude3", 17)))
    } catch {
      case e: Exception =>
        trans.rollback()
        throw e
    } finally {
      trans.close()
      fs.setTransaction(Transaction.AUTO_COMMIT)
    }
  }

  def removeInsideTransaction = {
    clearTablesHard()

    addFeatures(featureSeq)

    val trans = new DefaultTransaction("trans1")
    fs.setTransaction(trans)
    try {
      fs.removeFeatures(CQL.toFilter("name = 'will' or name='george' or name='sue'"))
      trans.commit()

      // everything
      val features = fs.getFeatures(Filter.INCLUDE).features().toSeq
      features must haveSize(2)
      features.map(f => f.getAttribute("name")) must containTheSameElementsAs(Seq("karen", "bob"))

      // geometric search
      val featuresBbox = fs.getFeatures(ECQL.toFilter("BBOX(geom, -170, -80, 170, 80")).features().toSeq
      featuresBbox must haveSize(2)
      featuresBbox.map(f => f.getAttribute("name")) must containTheSameElementsAs(Seq("karen", "bob"))
    } catch {
      case e: Exception =>
        trans.rollback()
        throw e
    } finally {
      trans.close()
      fs.setTransaction(Transaction.AUTO_COMMIT)
    }
  }

  def deleteWhenGeometryChanges = {
    clearTablesHard()

    addFeatures(featureSeq)

    val filter = CQL.toFilter("name = 'bob' or name = 'karen'")
    val writer = ds.getFeatureWriter(sftName, filter, Transaction.AUTO_COMMIT)

    while (writer.hasNext) {
      val sf = writer.next
      sf.setDefaultGeometry(WKTUtils.read("POINT(-50.0 -50.0)"))
      writer.write()
    }
    writer.close()

    // Verify old geo bbox doesn't return them
    val features45 = fs.getFeatures(ECQL.toFilter("BBOX(geom, 44.9,48.9,180.0,90.0)")).features().toSeq
    features45.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("will", "george", "sue"))

    // Verify that new geometries are written with a bbox query that uses the index
    val features50 = fs.getFeatures(ECQL.toFilter("BBOX(geom, -51, -51, -49, -49)")).features().toSeq
    features50.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("bob", "karen"))

    // get them all
    val all = fs.getFeatures(ECQL.toFilter("BBOX(geom, -170, -80, 170, 80)")).features().toSeq
    all.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("will", "george", "sue", "bob", "karen"))

    // get none
    val none = fs.getFeatures(ECQL.toFilter("BBOX(geom, 30.0,30.0,31.0,31.0)")).features().toSeq
    none must beEmpty
  }

  def deleteWhenDateChanges = {
    clearTablesHard()

    addFeatures(featureSeq)

    val filter = CQL.toFilter("name = 'will' or name='george'")
    val writer = ds.getFeatureWriter(sftName, filter, Transaction.AUTO_COMMIT)

    val newDate = sdf.parse("20100202")
    while (writer.hasNext) {
      val sf = writer.next
      sf.setAttribute("dtg", newDate)
      writer.write()
    }
    writer.close()

    // Verify old date range doesn't return them
    val jan = fs.getFeatures(ECQL.toFilter("dtg DURING 2013-12-29T00:00:00Z/2014-01-31T00:00:00Z")).features().toSeq
    jan.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("sue", "bob", "karen"))

    // Verify new date range returns things
    val feb = fs.getFeatures(ECQL.toFilter("dtg DURING 2010-02-01T00:00:00Z/2010-02-03T00:00:00Z")).features().toSeq
    feb.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("will","george"))

    // Verify large date range returns everything
    val all = fs.getFeatures(ECQL.toFilter("dtg DURING 2001-01-01T00:00:00Z/2015-02-03T00:00:00Z")).features().toSeq
    all.map(_.getAttribute("name")) must containTheSameElementsAs(Seq("will", "george", "sue", "bob", "karen"))

    // Verify other date range returns nothing
    val none = fs.getFeatures(ECQL.toFilter("dtg DURING 2014-01-01T00:00:00Z/2014-01-03T23:59:59Z")).features().toSeq
    none must beEmpty
  }

  // TODO this should be moved somewhere else...
  def verifyStartEndTimesAreExcluded = {
    clearTablesHard()

    addFeatures(featureSeq)

    val afterFilter = fs.getFeatures(ECQL.toFilter("dtg AFTER 2014-02-02T00:00:00Z")).features.toSeq
    afterFilter must beEmpty

    val beforeFilter = fs.getFeatures(ECQL.toFilter("dtg BEFORE 2014-01-02T00:00:00Z")).features.toSeq
    beforeFilter must beEmpty
  }

  def ensureIdsEndureWhenSpatialIndexChanges = {
    clearTablesHard()

    val toAdd = featureSeq
    addFeatures(featureSeq)

    val writer = ds.getFeatureWriter(sftName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
    val newDate = sdf.parse("20120102")
    while (writer.hasNext) {
      val sf = writer.next
      sf.setAttribute("dtg", newDate)
      sf.setDefaultGeometry(WKTUtils.read("POINT(10.0 10.0)"))
      writer.write()
    }
    writer.close()

    val features = fs.getFeatures(Filter.INCLUDE).features().toSeq

    features.size mustEqual toAdd.size

    val compare = features.sortBy(_.getID).zip(toAdd.sortBy(_.getID))
    forall(compare) { case (updated, original) =>
      updated.getID mustEqual original.getID
      updated.getDefaultGeometry must not be equalTo(original.getDefaultGeometry)
      updated.getAttribute("dtg") must not be equalTo(original.getAttribute("dtg"))
    }
  }

  def verifyDeleteAddSameKeyWorks = {
    clearTablesHard()

    addFeatures(featureSeq)

    val filter = CQL.toFilter("name = 'will'")

    val hints = ds.strategyHints(sft)
    val q = new Query(sft.getTypeName, filter)
    QueryStrategyDecider.chooseStrategies(sft, q, hints, None).head must beAnInstanceOf[AttributeIdxStrategy]

    import org.locationtech.geomesa.utils.geotools.Conversions._

    // Retrieve Will's ID before deletion.
    val featuresBeforeDelete = fs.getFeatures(filter).features().toSeq

    featuresBeforeDelete must haveSize(1)
    val willId = featuresBeforeDelete.head.getID

    fs.removeFeatures(filter)

    // NB: We really need a test which reads from the attribute table directly since missing records entries
    //  will result in attribute queries
    // This verifies that 'will' has been deleted from the attribute table.
    val attributeTableFeatures = fs.getFeatures(filter).features().toSeq
    attributeTableFeatures must beEmpty

    // This verifies that 'will' has been deleted from the record table.
    val recordTableFeatures =fs.getFeatures(ECQL.toFilter(s"IN('$willId')")).features().toSeq
    recordTableFeatures must beEmpty

    // This verifies that 'will' has been deleted from the ST idx table.
    val stTableFeatures = fs.getFeatures(ECQL.toFilter("BBOX(geom, 44.0,44.0,51.0,51.0)")).features().toSeq
    stTableFeatures.count(_.getID == willId) mustEqual 0

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    val geom = WKTUtils.read("POINT(10.0 10.0)")
    val date = sdf.parse("20120102")
    /* create a feature */
    featureCollection.add(AvroSimpleFeatureFactory.buildAvroFeature(sft, Seq("will", 56.asInstanceOf[AnyRef], date, geom), "fid1"))
    fs.addFeatures(featureCollection)

    val features =fs.getFeatures(filter).features().toSeq
    features must haveSize(1)
  }

  def createZ3BasedUUIDs = {
    clearTablesHard()

    // space out the adding slightly so we ensure they sort how we want - resolution is to the ms
    // also ensure we don't set use_provided_fid
    generateFeatures(reuseLocation = true, reuseDTG = true).foreach { f =>
      val featureCollection = new DefaultFeatureCollection(sftName, sft)
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.FALSE)
      f.getUserData.remove(Hints.PROVIDED_FID)
      featureCollection.add(f)
      // write the feature to the store
      fs.addFeatures(featureCollection)
      Thread.sleep(2)
    }

    val scanner = ds.connector.createScanner(ds.getTableName(sftName, RecordTable), new Authorizations)
    val serializer = new KryoFeatureSerializer(sft)
    val rows = scanner.toList
    scanner.close()

    // trim off table prefix to get the UUIDs
    val rowKeys = rows.map(_.getKey.getRow.toString).map(r => r.substring(r.length - 36))
    rowKeys must haveLength(5)

    // ensure that the z3 range is the same
    rowKeys.map(_.substring(0, 18)).toSet must haveLength(1)
    // ensure that the second part of the UUID is random
    rowKeys.map(_.substring(19)).toSet must haveLength(5)

    val ids = rows.map(e => serializer.deserialize(e.getValue.get).getID)
    ids must haveLength(5)
    forall(ids)(_ must not(beMatching("fid\\d")))
    // ensure they share a common prefix, since they have the same dtg/geom
    ids.map(_.substring(0, 18)).toSet must haveLength(1)
    // ensure that the second part of the UUID is random
    ids.map(_.substring(19)).toSet must haveLength(5)
  }

  def clearTablesHard(): Unit = {
    GeoMesaTable.getTables(sft).map(ds.getTableName(sft.getTypeName, _)).foreach { name =>
      val deleter = connector.createBatchDeleter(name, new Authorizations(), 5, new BatchWriterConfig())
      deleter.setRanges(Seq(new aRange()))
      deleter.delete()
      deleter.close()
    }
  }

  def fullTestSuite = {
    lock.synchronized {
      deleteAndRepopulate
      updateViaECQL
      removeFeatures
      addInsideTransaction
      removeInsideTransaction
      deleteWhenGeometryChanges
      deleteWhenDateChanges
      verifyStartEndTimesAreExcluded
      ensureIdsEndureWhenSpatialIndexChanges
      verifyDeleteAddSameKeyWorks
      createZ3BasedUUIDs
    }
  }

  "AccumuloFeatureWriter" should {
    "pass tests for all types" >> {
      val cn = getClass.getSimpleName

      for(aTest <- Seq(TestPoint, TestGeometry, TestGeometry_Af, TestGeometry_Aj, TestGeometry_Df)) yield {
        test = aTest

        println(s"\n\n[$cn $test STRATEGIES]\n\tt:  $TemporalStrategyOpt\n\ta:  $AgeStrategyOpt\n\tn:  $NameStrategyOpt\n\tg:  $GeospatialStrategyOpt\n")

        fullTestSuite
      }
    }
  }
}


object TestType extends Enumeration {
  type TestType = Value
  val TestPoint, TestGeometry, TestGeometry_Af, TestGeometry_Aj, TestGeometry_Df = Value

  def buildSpec(nameIndex: String = "full", ageIndex: String = "none", dateIndex: String = "none", geomType: String = "Geometry"): String =
    Seq(
      s"name:String:index=$nameIndex",
      s"age:Integer:index=$ageIndex",
      s"dtg:Date:index=$dateIndex",
      s"geom:$geomType:srid=4326:index=full"
    ).mkString(",")

  def getTestSpec(test: TestType): String = test match {
    case TestPoint    => buildSpec(geomType = "Point")
    case TestGeometry => buildSpec()
    case TestGeometry_Af => buildSpec(ageIndex = "full")
    case TestGeometry_Aj => buildSpec(ageIndex = "join")
    case TestGeometry_Df => buildSpec(dateIndex = "full")
  }
}
