package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.Longs
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.tables.Z2Table
import org.locationtech.sfcurve.zorder.Z2

class Z2Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z2Iterator.{PointsKey, ZKey}

  var source: SortedKeyValueIterator[Key, Value] = null
  var zNums: Array[Int] = null

  var xmin: Int = -1
  var xmax: Int = -1
  var ymin: Int = -1
  var ymax: Int = -1

  var isPoints: Boolean = false
  var rowToLong: Array[Byte] => Long = null

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)

    isPoints = options.get(PointsKey).toBoolean

    zNums = options.get(ZKey).split(":").map(_.toInt)
    xmin = zNums(0)
    xmax = zNums(1)
    ymin = zNums(2)
    ymax = zNums(3)
    rowToLong = rowToLong(if (isPoints) 8 else Z2Table.GEOM_Z_NUM_BYTES)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop && !inBounds(source.getTopKey, rowToLong)) {
      source.next()
    }
    if (source.hasTop) {
      topKey = source.getTopKey
      topValue = source.getTopValue
    }
  }

  private def inBounds(k: Key, getZ: (Array[Byte] => Long)): Boolean = {
    k.getRow(row)
    val bytes = row.getBytes
    val keyZ = getZ(bytes)
    val (x, y) = new Z2(keyZ).decode
    x >= xmin && x <= xmax && y >= ymin && y <= ymax
  }

  private def rowToLong(count: Int): (Array[Byte]) => Long = count match {
    case 3 => (bb) => Longs.fromBytes(bb(3), bb(4), bb(5), 0, 0, 0, 0, 0)
    case 4 => (bb) => Longs.fromBytes(bb(3), bb(4), bb(5), bb(6), 0, 0, 0, 0)
    case 8 => (bb) => Longs.fromBytes(bb(3), bb(4), bb(5), bb(6), bb(7), bb(8), bb(9), bb(10))
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val iter = new Z2Iterator
    val opts = Map(PointsKey -> isPoints.toString, ZKey -> zNums.mkString(":"))
    iter.init(source, opts, env)
    iter
  }
}

object Z2Iterator {

  val ZKey = "z"
  val PointsKey = "p"

  def configure(isPoints: Boolean, xmin: Int, xmax: Int, ymin: Int, ymax: Int, priority: Int) = {
    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])
    is.addOption(PointsKey, isPoints.toString)
    is.addOption(ZKey, s"$xmin:$xmax:$ymin:$ymax")
    is
  }
}