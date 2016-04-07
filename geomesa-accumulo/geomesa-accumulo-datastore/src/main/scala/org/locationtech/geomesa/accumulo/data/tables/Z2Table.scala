package org.locationtech.geomesa.accumulo.data.tables

import java.util.Map.Entry

import com.google.common.base.Charsets
import com.google.common.primitives.{Bytes, Longs}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.accumulo.index.QueryPlanners.FeatureFunction
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.simple.SimpleFeatureType

object Z2Table extends GeoMesaTable {
  import org.locationtech.geomesa.accumulo.data._
  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq
  val ZCURVE2D = new ZCurve2D(4096)
  val FULL_CF = new Text("F")

  /**
    * Is the table compatible with the given feature type
    */
  override def supports(sft: SimpleFeatureType): Boolean = sft.getGeometryDescriptor != null

  /**
    * The name used to identify the table
    */
  override def suffix: String = "z2_idx"

  // split(1 byte), week(2 bytes), z value (8 bytes), id (n bytes)
  private def getPointRowKey(ftw: FeatureToWrite): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.Conversions._
    val pt = ftw.feature.point
    val split = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val id = ftw.feature.getID.getBytes(Charsets.UTF_8)
    val z = ZCURVE2D.toIndex(pt.getX, pt.getY)
    Seq(Bytes.concat(split, Longs.toByteArray(z), id))
  }

  /**
    * Creates a function to write a feature to the table
    */
  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    // TODO: only works for points right now
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val getValue: (FeatureToWrite) => Value =
      if (sft.getSchemaVersion > 5) {
        // we know the data is kryo serialized in version 6+
        (fw) => fw.dataValue
      } else {
        // we always want to use kryo - reserialize the value to ensure it
        val writer = new KryoFeatureSerializer(sft)
        (fw) => new Value(writer.serialize(fw.feature))
      }

    (fw: FeatureToWrite) => {
      val rows = getPointRowKey(fw)
      // store the duplication factor in the column qualifier for later use
      val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
      rows.map { row =>
        val mutation = new Mutation(row)
        mutation.put(FULL_CF, cq, fw.columnVisibility, getValue(fw))
        mutation
      }
    }

  }

  /**
    * Creates a function to delete a feature to the table
    */
  override def remover(sft: SimpleFeatureType): FeatureToMutations = ???

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
  }

  def adaptZ2KryoIterator(sft: SimpleFeatureType): FeatureFunction = {
    val kryo = new KryoFeatureSerializer(sft)
    (e: Entry[Key, Value]) => {
      // TODO lazy features if we know it's read-only?
      kryo.deserialize(e.getValue.get())
    }
  }

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
  // geoms always have splits, but they weren't added until schema 7
  def hasSplits(sft: SimpleFeatureType) = sft.getSchemaVersion > 6

}
