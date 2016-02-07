package org.locationtech.geomesa.cassandra.data


import com.datastax.driver.core._
import org.geotools.data.store._
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.cassandra.data.CassandraDataStore.FieldSerializer
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.SimpleFeatureType

class CassandraContentState(entry: ContentEntry, val session: Session, val tableMetadata: TableMetadata) extends ContentState(entry) {

  import scala.collection.JavaConversions._

  val sft: SimpleFeatureType = CassandraDataStore.getSchema(entry.getName, tableMetadata)
  val attrNames = sft.getAttributeDescriptors.map(_.getLocalName)
  val selectClause = (Array("fid") ++ attrNames).mkString(",")
  val table = sft.getTypeName
  val deserializers = sft.getAttributeDescriptors.map { ad => FieldSerializer(ad) }
  val geoTimeQuery =  session.prepare(s"select $selectClause from $table where (pkz = ?) and (z31 >= ?) and (z31 <= ?)")
  val builderPool = ObjectPoolFactory(getBuilder, 10)

  private def getBuilder = {
    val builder = new SimpleFeatureBuilder(sft)
    builder.setValidating(java.lang.Boolean.FALSE)
    builder
  }

}
