package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.Longs
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range => AccRange, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.sfcurve.zorder.Z2

/**
  * Created by afox on 4/7/16.
  */
class Z2Iterator extends SortedKeyValueIterator[Key, Value] {
  import org.locationtech.geomesa.accumulo.iterators.Z2Iterator.{pointsKey, splitsKey, zKey}

  var source: SortedKeyValueIterator[Key, Value] = null
  var zNums: Array[Int] = null

  var xmin: Int = -1
  var xmax: Int = -1
  var ymin: Int = -1
  var ymax: Int = -1

  var isPoints: Boolean = false
  var hasSplits: Boolean = false
  var rowToLong: Array[Byte] => Long = null

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop && !inBounds(source.getTopKey, rowToLong)) { source.next() }
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
    case 3 if hasSplits => (bb) => Longs.fromBytes(bb(3), bb(4), bb(5), 0, 0, 0, 0, 0)
    case 3              => (bb) => Longs.fromBytes(bb(2), bb(3), bb(4), 0, 0, 0, 0, 0)
    case 4 if hasSplits => (bb) => Longs.fromBytes(bb(3), bb(4), bb(5), bb(6), 0, 0, 0, 0)
    case 4              => (bb) => Longs.fromBytes(bb(2), bb(3), bb(4), bb(5), 0, 0, 0, 0)
    case 8 if hasSplits => (bb) => Longs.fromBytes(bb(3), bb(4), bb(5), bb(6), bb(7), bb(8), bb(9), bb(10))
    case 8              => (bb) => Longs.fromBytes(bb(2), bb(3), bb(4), bb(5), bb(6), bb(7), bb(8), bb(9))
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)

    isPoints = options.get(pointsKey).toBoolean
    hasSplits = options.get(splitsKey).toBoolean

    zNums = options.get(zKey).split(":").map(_.toInt)
    xmin = zNums(0)
    xmax = zNums(1)
    ymin = zNums(2)
    ymax = zNums(3)
    rowToLong = rowToLong(8)
  }

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val iter = new Z2Iterator
    val opts = Map(pointsKey -> isPoints.toString, splitsKey -> hasSplits.toString, zKey -> zNums.mkString(":"))
    iter.init(source, opts, env)
    iter
  }
}

object Z2Iterator {

  val zKey = "z"
  val pointsKey = "p"
  val splitsKey = "s"

  def configure(isPoints: Boolean,
                xmin: Int,
                xmax: Int,
                ymin: Int,
                ymax: Int,
                splits: Boolean,
                priority: Int) = {
    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])
    is.addOption(pointsKey, isPoints.toString)
    is.addOption(zKey, s"$xmin:$xmax:$ymin:$ymax")
    is.addOption(splitsKey, splits.toString)
    is
  }
}