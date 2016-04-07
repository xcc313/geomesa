/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.tables

import com.google.common.base.Charsets
import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.google.common.primitives.{Bytes, Longs}
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, LineString, Point}
import org.apache.accumulo.core.client.admin.TableOperations
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.{FeatureToMutations, FeatureToWrite}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.sfcurve.zorder.Z3.ZPrefix
import org.locationtech.sfcurve.zorder.{Z2, ZCurve2D}
import org.opengis.feature.simple.SimpleFeatureType

object Z2Table extends GeoMesaTable {
  import org.locationtech.geomesa.accumulo.data._

  val ZCURVE2D = new ZCurve2D(4096)
  val FULL_CF = new Text("F")
  val BIN_CF = new Text("B")
  val EMPTY_BYTES = Array.empty[Byte]
  val EMPTY_VALUE = new Value(EMPTY_BYTES)
  val NUM_SPLITS = 4 // can't be more than Byte.MaxValue (127)
  val SPLIT_ARRAYS = (0 until NUM_SPLITS).map(_.toByte).toArray.map(Array(_)).toSeq

  // the bytes of z we keep for complex geoms
  // 3 bytes is 15 bits of geometry (not including time bits and the first 2 bits which aren't used)
  // roughly equivalent to 3 digits of geohash (32^3 == 2^15) and ~78km resolution
  // (4 bytes is 20 bits, equivalent to 4 digits of geohash and ~20km resolution)
  // note: we also lose time resolution
  val GEOM_Z_NUM_BYTES = 3
  // mask for zeroing the last (8 - GEOM_Z_NUM_BYTES) bytes
  val GEOM_Z_MASK: Long =
    java.lang.Long.decode("0x" + Array.fill(GEOM_Z_NUM_BYTES)("ff").mkString) << (8 - GEOM_Z_NUM_BYTES) * 8

  // TODO enable nonpoints
  override def supports(sft: SimpleFeatureType): Boolean =
    sft.isPoints && sft.getGeometryDescriptor != null && sft.getSchemaVersion > 7

  override def suffix: String = "z2_idx"

  override def writer(sft: SimpleFeatureType): FeatureToMutations = {
    val getRowKeys: (FeatureToWrite) => Seq[Array[Byte]] = if (sft.isPoints) getPointRowKey else getGeomRowKeys

    (fw: FeatureToWrite) => {
      val rows = getRowKeys(fw)
      // store the duplication factor in the column qualifier for later use
      val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
      rows.map { row =>
        val mutation = new Mutation(row)
        mutation.put(FULL_CF, cq, fw.columnVisibility, fw.dataValue)
        fw.binValue.foreach(v => mutation.put(BIN_CF, cq, fw.columnVisibility, v))
        mutation
      }
    }
  }

  override def remover(sft: SimpleFeatureType): FeatureToMutations = {
    val getRowKeys: (FeatureToWrite) => Seq[Array[Byte]] = if (sft.isPoints) getPointRowKey else getGeomRowKeys

    (fw: FeatureToWrite) => {
      val rows = getRowKeys(fw)
      val cq = if (rows.length > 1) new Text(Integer.toHexString(rows.length)) else EMPTY_TEXT
      rows.map { row =>
        val mutation = new Mutation(row)
        mutation.putDelete(BIN_CF, cq, fw.columnVisibility)
        mutation.putDelete(FULL_CF, cq, fw.columnVisibility)
        mutation
      }
    }
  }


  // split(1 byte), z value (8 bytes), id (n bytes)
  private def getPointRowKey(ftw: FeatureToWrite): Seq[Array[Byte]] = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
    val split = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val id = ftw.feature.getID.getBytes(Charsets.UTF_8)
    val pt = ftw.feature.point
    val z = ZCURVE2D.toIndex(pt.getX, pt.getY)
    Seq(Bytes.concat(split, Longs.toByteArray(z), id))
  }

  // split(1 byte), z value (3 bytes), id (n bytes)
  private def getGeomRowKeys(ftw: FeatureToWrite): Seq[Array[Byte]] = {
    val split = SPLIT_ARRAYS(ftw.idHash % NUM_SPLITS)
    val geom = ftw.feature.getDefaultGeometry.asInstanceOf[Geometry]
    val zs = zBox(geom)
    val id = ftw.feature.getID.getBytes(Charsets.UTF_8)
    zs.map(z => Bytes.concat(split, Longs.toByteArray(z).take(GEOM_Z_NUM_BYTES), id)).toSeq
  }


  // gets a sequence of z values that cover the geometry
  private def zBox(geom: Geometry): Set[Long] = geom match {
    case g: Point => Set(ZCURVE2D.toIndex(g.getX, g.getY))
    case g: LineString =>
      // we flatMap bounds for each line segment so we cover a smaller area
      (0 until g.getNumPoints).map(g.getPointN).sliding(2).flatMap { case Seq(one, two) =>
        val (xmin, xmax) = minMax(one.getX, two.getX)
        val (ymin, ymax) = minMax(one.getY, two.getY)
        zBox(xmin, ymin, xmax, ymax)
      }.toSet
    case g: GeometryCollection => (0 until g.getNumGeometries).toSet.map(g.getGeometryN).flatMap(zBox)
    case g: Geometry =>
      val env = g.getEnvelopeInternal
      zBox(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
  }

  // gets a sequence of z values that cover the bounding box
  private def zBox(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Set[Long] = {
    val zmin = ZCURVE2D.toIndex(xmin, ymin)
    val zmax = ZCURVE2D.toIndex(xmax, ymax)
    getZPrefixes(zmin, zmax)
  }

  private def minMax(a: Double, b: Double): (Double, Double) = if (a < b) (a, b) else (b, a)

  // gets z values that cover the interval
  private def getZPrefixes(zmin: Long, zmax: Long): Set[Long] = {
    val in = scala.collection.mutable.Queue((zmin, zmax))
    val out = scala.collection.mutable.HashSet.empty[Long]

    while (in.nonEmpty) {
      val (min, max) = in.dequeue()
      val ZPrefix(zprefix, zbits) = Z2.longestCommonPrefix(min, max)
      if (zbits < GEOM_Z_NUM_BYTES * 8) {
        // divide the range into two smaller ones using tropf litmax/bigmin
        val (litmax, bigmin) = Z2.zdivide(Z2((min + max) / 2), Z2(min), Z2(max))
        in.enqueue((min, litmax.z), (bigmin.z, max))
      } else {
        // we've found a prefix that contains our z range
        // truncate down to the bytes we use so we don't get dupes
        out.add(zprefix & GEOM_Z_MASK)
      }
    }

    out.toSet
  }

  // reads the feature ID from the row key
  def getIdFromRow(sft: SimpleFeatureType): (Array[Byte]) => String = {
    val offset = if (sft.isPoints) 11 else 1 + GEOM_Z_NUM_BYTES
    (row: Array[Byte]) => new String(row, offset, row.length - offset, Charsets.UTF_8)
  }

  override def configureTable(sft: SimpleFeatureType, table: String, tableOps: TableOperations): Unit = {
    import scala.collection.JavaConversions._

    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "128M")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val localityGroups = Seq(BIN_CF, FULL_CF).map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    tableOps.setLocalityGroups(table, localityGroups)

    // drop first split, otherwise we get an empty tablet
    val splits = SPLIT_ARRAYS.drop(1).map(new Text(_)).toSet
    val splitsToAdd = splits -- tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }
  }
}
