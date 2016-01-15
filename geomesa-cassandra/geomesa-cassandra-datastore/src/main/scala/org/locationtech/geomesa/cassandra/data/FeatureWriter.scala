package org.locationtech.geomesa.cassandra.data

import java.io.Serializable
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util
import java.util.UUID

import com.datastax.driver.core.{Cluster, Session}
import com.google.common.primitives.{Ints, Longs}
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentFeatureStore, ContentFeatureSource, ContentEntry, ContentDataStore}
import org.geotools.data.{FeatureWriter => FW, FeatureReader, Query, DataStore, AbstractDataStoreFactory}
import org.geotools.feature.NameImpl
import org.geotools.filter.spatial.BBOXImpl
import org.geotools.geometry.jts.{ReferencedEnvelope, JTSFactoryFinder}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Random

object Test {
  val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
  val session = cluster.connect("test")

}

class CassandraDataStore extends ContentDataStore {
  import scala.collection.JavaConversions._

  override def createFeatureSource(contentEntry: ContentEntry): ContentFeatureSource = ???

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

  val s = new DateTime("2015-02-01")
  val e = new DateTime("2015-02-03")

  val (lx, ly, ux, uy) = (35.0, 35.0, 40.0, 40.0)

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val sew = epochWeeks(s)
    val eew = epochWeeks(e)

    (sew to eew).map { dt =>

    }
  }
}

class CassandraDataStoreFactory extends AbstractDataStoreFactory {
  override def createDataStore(map: util.Map[String, Serializable]): DataStore = ???

  override def createNewDataStore(map: util.Map[String, Serializable]): DataStore = ???

  override def getDescription: String = ""

  override def getParametersInfo: Array[Param] = Array()
}

class FeatureWriter(sft: SimpleFeatureType, session: Session) extends FW[SimpleFeatureType, SimpleFeature] {
  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  val stmt = session.prepare("INSERT INTO geo (feature_id, z30, z31, geom, dtg) VALUES(?, ?, ?, ?, ?)")

  import org.locationtech.geomesa.utils.geotools.Conversions._

  private val SFC = new Z3SFC
  private val SFC2D = new
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
    curFeature.setDefaultGeometry(gf.createPoint(new Coordinate(-180.0+Random.nextDouble()*360.0, -90.0 + Random.nextDouble()*180)))
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
    val prefix = weeks.getWeeks << 16

    val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
    val z3 = SFC.index(x, y, secondsInWeek)
    val z30 = (z3.z >> 54).toInt
    val z31 = z3.z

    println(s"dtg = $dtg")
    session.execute(stmt.bind(curFeature.getID, Int.box(prefix + z30), Long.box(z31): java.lang.Long, geo, dtg.toDate))
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}
}
