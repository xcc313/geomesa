package org.locationtech.geomesa.cassandra.data

import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core.Session
import org.apache.commons.codec.binary.Base64
import org.geotools.data.{FeatureWriter => FW}
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Random

class FeatureWriter(sft: SimpleFeatureType, session: Session) extends FW[SimpleFeatureType, SimpleFeature] {
 val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  val stmt = session.prepare("INSERT INTO geo (feature_id, z3, weekEpoch, geom, dtg) VALUES(?, ?, ?, ?, ?)")

  import org.locationtech.geomesa.utils.geotools.Conversions._

  private val SFC = new Z3SFC
  private var curFeature: SimpleFeature = null
  private val dtgIndex = 4
/*
    sft.getAttributeDescriptors
      .zipWithIndex
      .find { case (ad, idx) => classOf[java.util.Date].equals(ad.getType.getBinding) }
      .map  { case (_, idx)  => idx }
      .getOrElse(throw new RuntimeException("No date attribute"))
*/

//  private val encoder = new KryoFeatureSerializer(sft)

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature("", sft)
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
    println(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])
    val dtg = rDate
    val weeks = epochWeeks(dtg)
    val prefix = Int.box(weeks.getWeeks)

    val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
    val z3 = SFC.index(x, y, secondsInWeek)

    println(s"dtg = $dtg")
    session.execute(stmt.bind(curFeature.getID, Long.box(z3.z), prefix, geo, dtg.toDate))
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}
}
