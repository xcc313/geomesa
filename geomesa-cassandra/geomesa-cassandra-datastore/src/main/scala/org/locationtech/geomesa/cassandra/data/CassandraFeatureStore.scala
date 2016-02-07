package org.locationtech.geomesa.cassandra.data


import com.datastax.driver.core._
import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store._
import org.geotools.data.{FeatureWriter => FW, _}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class CassandraFeatureStore(entry: ContentEntry) extends ContentFeatureStore(entry, Query.ALL) {

  private lazy val contentState = entry.getState(getTransaction).asInstanceOf[CassandraContentState]

  override def getWriterInternal(query: Query, i: Int): FW[SimpleFeatureType, SimpleFeature] =
    new CassandraFeatureWriter(contentState.sft, contentState.session)

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  override def getCountInternal(query: Query): Int = 10 // TODO: fix

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    // TODO: currently we assume that the query has a dtg between predicate and a bbox
    val re = query.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, DefaultGeographicCRS.WGS84).asInstanceOf[Envelope]
    val (lx, ly, ux, uy) = (re.getMinX, re.getMinY, re.getMaxX, re.getMaxY)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), contentState.sft)
    val interval = FilterHelper.extractInterval(dtgFilters, contentState.sft.getDtgField)
    val startWeek = CassandraPrimaryKey.epochWeeks(interval.getStart)
    val sew = startWeek.getWeeks
    val endWeek = CassandraPrimaryKey.epochWeeks(interval.getEnd)
    val eew = endWeek.getWeeks

    val rows =
      (sew to eew).map { dt =>
        val dtshift = dt << 16

        val dtg = new DateTime(0).plusWeeks(dt)

        val seconds =
          if(dt != sew && dt != eew) {
            (0, CassandraPrimaryKey.ONE_WEEK_IN_SECONDS)
          } else {
            val starts =
              if(dt == sew) CassandraPrimaryKey.secondsInCurrentWeek(interval.getStart)
              else 0
            val ends =
              if(dt == eew) CassandraPrimaryKey.secondsInCurrentWeek(interval.getEnd)
              else CassandraPrimaryKey.ONE_WEEK_IN_SECONDS
            (starts, ends)
          }
        val zranges = org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC2D.toRanges(lx, ly, ux, uy)
        println(zranges)
        val shiftedRanges = zranges.flatMap { case (l, u, _) => (l to u).map { i => (dtshift + i).toInt } }
        (seconds, shiftedRanges)
      }

    val features = contentState.builderPool.withResource { builder =>
      val futures = rows.flatMap { case ((s, e), rowRanges) =>
        val z3ranges =
          org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC3D.ranges((lx, ux), (ly, uy), (s, e))
        rowRanges.flatMap { r =>
          z3ranges.map { case (l, u, contains) =>
            val q = contentState.geoTimeQuery.bind(r: java.lang.Integer, l: java.lang.Long, u: java.lang.Long)
            (r, contains, contentState.session.executeAsync(q))
          }
        }
      }
      futures.flatMap { case (r, contains, fut) =>
        val featureIterator = fut.get().iterator().map { r => convertRowToSF(r, builder) }
        val filt = query.getFilter
        val iter =
          if (!contains) featureIterator.filter(f => filt.evaluate(f)).toList
          else featureIterator.toList
        println(s"$r (${iter.length}")
        iter
      }
    }

    new DelegateSimpleFeatureReader(contentState.sft, new DelegateSimpleFeatureIterator(features.iterator))
  }

  def convertRowToSF(r: Row, builder: SimpleFeatureBuilder): SimpleFeature = {
    val attrs = contentState.deserializers.zipWithIndex.map { case (d, idx) => d.deserialize(r.getObject(idx + 1)) }
    val fid = r.getString(0)
    builder.reset()
    builder.buildFeature(fid, attrs.toArray)
  }
}
