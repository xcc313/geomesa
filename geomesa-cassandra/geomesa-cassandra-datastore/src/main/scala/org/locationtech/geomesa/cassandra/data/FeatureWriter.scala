package org.locationtech.geomesa.cassandra.data

import java.io.Serializable
import java.nio.ByteBuffer
import java.util
import java.util.UUID

import com.datastax.driver.core.{Cluster, Row, Session}
import com.vividsolutions.jts.geom.{Envelope, Coordinate}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource, ContentFeatureStore}
import org.geotools.data.{AbstractDataStoreFactory, DataStore, FeatureReader, FeatureWriter => FW, Query}
import org.geotools.feature.NameImpl
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.{JTSFactoryFinder, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Random

object Test {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("test")

}

class CassandraDataStore(session: Session) extends ContentDataStore {
  import scala.collection.JavaConversions._

  override def createFeatureSource(contentEntry: ContentEntry): ContentFeatureSource =
    new CassandraFeatureStore(contentEntry, session)

  override def createTypeNames(): util.List[Name] = List(new NameImpl("foo"))
}

class CassandraFeatureStore(entry: ContentEntry, session: Session) extends ContentFeatureStore(entry, Query.ALL) {
  val sft = SimpleFeatureTypes.createType("foo", "featureid:String,*geom:Point:srid=4326,dtg:Date")
  override def getWriterInternal(query: Query, i: Int): FW[SimpleFeatureType, SimpleFeature] = ???

  override def buildFeatureType(): SimpleFeatureType = sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  override def getCountInternal(query: Query): Int = 10

  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    // TODO: currently we assume that the query has a dtg between predicate and a bbox
    val re = query.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, DefaultGeographicCRS.WGS84).asInstanceOf[Envelope]
    val (lx, ly, ux, uy) = (re.getMinX, re.getMinY, re.getMaxX, re.getMaxY)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), sft)
    val interval = FilterHelper.extractInterval(dtgFilters, sft.getDtgField)
    val sew = epochWeeks(interval.getStart).getWeeks
    val eew = epochWeeks(interval.getEnd).getWeeks



    val rows =
      (sew to eew).flatMap { dt =>
        val dtshift = dt << 16

        val dtg = new DateTime(0).plusWeeks(dt)
        val minz = CassandraPrimaryKey(dtg, re.getMinX, re.getMinY)
        val maxz = CassandraPrimaryKey(dtg, re.getMaxX, re.getMaxY)

        val zranges = org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC2D.toRanges(minz.x, minz.y, maxz.x, maxz.y)
        zranges.flatMap { case (l, u) => (l to u).map { i => dtshift + i } }
      }

    val z3ranges =
      org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC3D.ranges((lx, ux), (ly, uy), (0, Weeks.weeks(1).toStandardSeconds.getSeconds))
        .map { case (l, r) => s"(z31 >= $l) and (z31 <= $r)" }

    import scala.collection.JavaConversions._

    val features = rows.flatMap { r =>
      z3ranges.flatMap { z =>
        val q = s"select * from geo where (pkz = $r) AND $z"
        println(q)
        session.execute(q).all()
      }
    }.map(convertRowToSF)

    new DelegateSimpleFeatureReader(sft, new DelegateSimpleFeatureIterator(features.iterator))
  }

  def convertRowToSF(r: Row): SimpleFeature = {
    println(r)
    null
  }
}

class CassandraDataStoreFactory extends AbstractDataStoreFactory {

  override def createDataStore(map: util.Map[String, Serializable]): DataStore = {
    val cp = CONTACT_POINT.lookUp(map).asInstanceOf[String]
    val ks = KEYSPACE.lookUp(map).asInstanceOf[String]
    val cluster = Cluster.builder().addContactPoint(cp).build()
    val session = cluster.connect(ks)
    new CassandraDataStore(session)
  }


  override def createNewDataStore(map: util.Map[String, Serializable]): DataStore = ???

  override def getDescription: String = ""

  override def getParametersInfo: Array[Param] = Array(CONTACT_POINT, KEYSPACE)

  val CONTACT_POINT = new Param("geomesa.cassandra.contact.point", classOf[String])
  val KEYSPACE      = new Param("geomesa.cassandra.keyspace", classOf[String])
}


object CassandraPrimaryKey {

  case class Key(idx: Int, x: Double, y: Double, dk: Int, z: Int)

  def unapply(idx: Int): Key = {
    val dk = idx >> 16
    val z = idx & 0x000000ff
    val (x, y) = SFC2D.toPoint(z)
    Key(idx, x, y, dk, z)
  }

  def apply(dtg: DateTime, x: Double, y: Double): Key = {
    val dk = epochWeeks(dtg).getWeeks << 16
    val z = SFC2D.toIndex(x, y).toInt
    val (rx, ry) = SFC2D.toPoint(z)
    val idx = dk + z
    Key(idx, rx, ry, dk, z)
  }

  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  val SFC2D = new ZCurve2D(math.pow(2,5).toInt)
  val SFC3D = new Z3SFC
}

class FeatureWriter(sft: SimpleFeatureType, session: Session) extends FW[SimpleFeatureType, SimpleFeature] {
  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  val stmt = session.prepare("INSERT INTO geo (feature_id, pkz, z31, x, y, geom, dtg) VALUES(?, ?, ?, ?, ?, ?, ?)")

  import org.locationtech.geomesa.utils.geotools.Conversions._

  private val SFC = new Z3SFC
  private var curFeature: SimpleFeature = null
//  private val dtgIndex = 4
/*
    sft.getAttributeDescriptors
      .zipWithIndex
      .find { case (ad, idx) => classOf[java.util.Date].equals(ad.getType.getBinding) }
      .map  { case (_, idx)  => idx }
      .getOrElse(throw new RuntimeException("No date attribute"))
*/

//  private val encoder = new KryoFeatureSerializer(sft)

  val gf = JTSFactoryFinder.getGeometryFactory
  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)
    curFeature.setDefaultGeometry(gf.createPoint(new Coordinate(-180.0+Random.nextDouble()*360.0, -90.0 + Random.nextDouble()*180.0)))
    curFeature
  }

  override def remove(): Unit = ???

  override def hasNext: Boolean = true

  def rDate = (new DateTime("2015-01-01")).plusSeconds((Random.nextDouble*60*60*24*180).toInt)

  override def write(): Unit = {
    // write
    val geom = curFeature.point
    val geo = ByteBuffer.wrap(WKBUtils.write(geom))
    val x = geom.getX
    val y = geom.getY
  //  println(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])
    val dtg = rDate
    val weeks = epochWeeks(dtg)

    val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
    val pk = CassandraPrimaryKey(dtg, x, y)
    val z3 = SFC.index(x, y, secondsInWeek)
    val z31 = z3.z

    println(s"dtg = $dtg")
    session.execute(stmt.bind(curFeature.getID, Int.box(pk.idx), Long.box(z31): java.lang.Long, Double.box(x), Double.box(y), geo, dtg.toDate))
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}
}
