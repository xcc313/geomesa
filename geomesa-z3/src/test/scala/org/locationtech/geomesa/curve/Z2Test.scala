/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.curve

import org.junit.runner.RunWith
import org.locationtech.sfcurve.CoveredRange
import org.locationtech.sfcurve.zorder.Z2
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class Z2Test extends Specification {

  val rand = new Random(-574)
  val maxInt = Z2SFC.lon.precision.toInt
  def nextDim() = rand.nextInt(maxInt)

  def padTo(s: String) = (new String(Array.fill(63)('0')) + s).takeRight(63)

  "Z2" should {

    "apply and unapply" >> {
      val (x, y) = (nextDim(), nextDim())
      val z = Z2(x, y)
      z match { case Z2(zx, zy) =>
        zx mustEqual x
        zy mustEqual y
      }
    }

    "apply and unapply min values" >> {
      val (x, y) = (0, 0)
      val z = Z2(x, y)
      z match { case Z2(zx, zy) =>
        zx mustEqual x
        zy mustEqual y
      }
    }

    "apply and unapply max values" >> {
      val Z2curve = Z2SFC
      val (x, y) = (Z2curve.lon.precision, Z2curve.lat.precision)
      val z = Z2(x.toInt, y.toInt)
      z match { case Z2(zx, zy) =>
        zx mustEqual x
        zy mustEqual y
      }
    }

    "split" >> {
      val splits = Seq(
        0x00000000ffffffL,
        0x00000000000000L,
        0x00000000000001L,
        0x000000000c0f02L,
        0x00000000000802L
      ) ++ (0 until 10).map(_ => nextDim().toLong)
      splits.foreach { l =>
        val expected = padTo(new String(l.toBinaryString.toCharArray.flatMap(c => s"0$c")))
        padTo(Z2.split(l).toBinaryString) mustEqual expected
      }
      success
    }

    "split and combine" >> {
      val z = nextDim()
      val split = Z2.split(z)
      val combined = Z2.combine(split)
      combined.toInt mustEqual z
    }

//    "support mid" >> {
//      val (x, y)   = (0, 0)
//      val (x2, y2) = (2, 2)
//      Z2(x, y).mid(Z2(x2, y2)) match {
//        case Z2(midx, midy) =>
//          midx mustEqual 1
//          midy mustEqual 1
//      }
//    }

    "support bigmin" >> {
      val zmin = Z2(2, 2)
      val zmax = Z2(3, 6)
      val f = Z2(5, 1)
      val (_, bigmin) = Z2.zdivide(f, zmin, zmax)
      bigmin match {
        case Z2(xhi, yhi) =>
          xhi mustEqual 2
          yhi mustEqual 4
      }
    }

    "support litmax" >> {
      val zmin = Z2(2, 2)
      val zmax = Z2(3, 6)
      val f = Z2(1, 7)
      val (litmax, _) = Z2.zdivide(f, zmin, zmax)
      litmax match {
        case Z2(xlow, ylow) =>
          xlow mustEqual 3
          ylow mustEqual 5
      }
    }

//    "support in range" >> {
//      val (x, y) = (nextDim(), nextDim())
//      val Z2 = Z2(x, y)
//      val lessx  = Z2(x - 1, y)
//      val lessx2 = Z2(x - 2, y)
//      val lessy  = Z2(x, y - 1)
//      val lessy2 = Z2(x, y - 2)
//      val less1  = Z2(x - 1, y - 1)
//      val less2  = Z2(x - 2, y - 2)
//      val morex  = Z2(x + 1, y)
//      val morex2 = Z2(x + 2, y)
//      val morey  = Z2(x, y + 1)
//      val more1  = Z2(x + 1, y + 1)
//
//      Z2.inRange(lessx, morex) must beTrue
//      Z2.inRange(lessx, morey) must beTrue
//      Z2.inRange(lessx, morez) must beTrue
//      Z2.inRange(lessx, more1) must beTrue
//
//      Z2.inRange(lessy, morex) must beTrue
//      Z2.inRange(lessy, morey) must beTrue
//      Z2.inRange(lessy, morez) must beTrue
//      Z2.inRange(lessy, more1) must beTrue
//
//      Z2.inRange(lessz, morex) must beTrue
//      Z2.inRange(lessz, morey) must beTrue
//      Z2.inRange(lessz, morez) must beTrue
//      Z2.inRange(lessz, more1) must beTrue
//
//      Z2.inRange(less1, more1) must beTrue
//
//      Z2.inRange(more1, less1) must beFalse
//      Z2.inRange(morex, morex2) must beFalse
//      Z2.inRange(lessx2, lessx) must beFalse
//      Z2.inRange(lessy2, lessy) must beFalse
//      Z2.inRange(lessz2, lessx) must beFalse
//      Z2.inRange(less2, less1) must beFalse
//      Z2.inRange(less2, more1) must beTrue
//    }

    "calculate ranges" >> {
      val min = Z2(2, 2)
      val max = Z2(3, 6)
      val ranges = Z2.zranges(min, max)
      ranges must haveLength(3)
      ranges must containTheSameElementsAs(
        Seq(
          CoveredRange(Z2(2, 2).z, Z2(3, 3).z),
          CoveredRange(Z2(2, 4).z, Z2(3, 5).z),
          CoveredRange(Z2(2, 6).z, Z2(3, 6).z)
        )
      )
    }

    "return non-empty ranges for a number of cases" >> {
      val sfc = Z2SFC

      val ranges = Seq(
        (sfc.index(-180, -90),      sfc.index(180, 90)),        // whole world
        (sfc.index(-90, -45),       sfc.index(90, 45)),         // half world
        (sfc.index(35, 65),         sfc.index(45, 75)),         // 10^2 degrees
        (sfc.index(35, 55),         sfc.index(45, 75)),         // 10x20 degrees
        (sfc.index(35, 65),         sfc.index(37, 68)),         // 2x3 degrees
        (sfc.index(35, 65),         sfc.index(40, 70)),         // 5^2 degrees
        (sfc.index(39.999, 60.999), sfc.index(40.001, 61.001)), // small bounds
        (sfc.index(51.0, 51.0),     sfc.index(51.1, 51.1)),     // small bounds
        (sfc.index(51.0, 51.0),     sfc.index(51.001, 51.001)), // small bounds
        (Z2(sfc.index(51.0, 51.0).z - 1), Z2(sfc.index(51.0, 51.0).z + 1)) // 62 bits in common
      )

      def print(l: Z2, u: Z2, size: Int): Unit =
        println(s"${round(sfc.invert(l))} ${round(sfc.invert(u))}\t$size")
      def round(z: (Double, Double)): (Double, Double) =
        (math.round(z._1 * 1000.0) / 1000.0, math.round(z._2 * 1000.0) / 1000.0)

      forall(ranges) { r =>
        val ret = Z2.zranges(r._1, r._2)
        print(r._1, r._2, ret.length)
        ret.length must beGreaterThan(0)
      }
    }
  }
}
