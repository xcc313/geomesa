package org.locationtech.geomesa.process

import com.vividsolutions.jts.geom.Point
import org.geotools.data.DataUtilities
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

@DescribeProcess(title = "Point2PointProcess", description = "Aggregates a collection of points into a linestring.")
class Point2PointProcess extends VectorProcess {

  private val sft = SimpleFeatureTypes.createType("geomesa", "point2point", "length:Double,geom:LineString:srid=4326")
  private val gf = JTSFactoryFinder.getGeometryFactory

  @DescribeResult(name = "result", description = "Aggregated feature collection")
  def execute(

               @DescribeParameter(name = "data", description = "Input feature collection")
               data: SimpleFeatureCollection,

               @DescribeParameter(name = "groupingField", description = "Field on which to group")
               groupingField: String,

               @DescribeParameter(name = "sortField", description = "Field on which to sort")
               sortField: String,

               @DescribeParameter(name = "minimumNumberOfPoints", description = "Minimum number of points with which to create a track")
               minPoints: Int,

               @DescribeParameter(name = "breakOnDay", description = "Break connections on day marks")
               breakOnDay: Boolean

               ): SimpleFeatureCollection = {

    import org.locationtech.geomesa.utils.geotools.Conversions._
    val builder = new SimpleFeatureBuilder(sft)

    val groupingFieldIndex = data.getSchema.indexOf(groupingField)
    val sortFieldIndex = data.getSchema.indexOf(sortField)

    val lineFeatures =
      data.features().toList
        .groupBy(_.get(groupingFieldIndex).asInstanceOf[String])
        .filter { case (_, coll) => coll.size > minPoints }
        .flatMap { case (group, coll) =>

        val globalSorted = coll.sortBy(_.get(sortFieldIndex).asInstanceOf[java.util.Date])

        val groups =
          if(!breakOnDay) Array(globalSorted)
          else globalSorted.groupBy { f => new DateTime(f.get(sortFieldIndex).asInstanceOf[java.util.Date]).dayOfYear() }.map { case (_, g) => g }.toArray

        groups.map { sorted =>
          val pts = sorted.map(_.getDefaultGeometry.asInstanceOf[Point].getCoordinate)
          val ls = gf.createLineString(pts.toArray)
          val length = pts.sliding(2, 1).map { case List(s, e) => JTS.orthodromicDistance(s, e, DefaultGeographicCRS.WGS84) }.sum
          val sf = builder.buildFeature(group)
          sf.setAttributes(Array[AnyRef](Double.box(length), ls))
          sf
        }
      }

    DataUtilities.collection(lineFeatures.toArray)
  }
}
