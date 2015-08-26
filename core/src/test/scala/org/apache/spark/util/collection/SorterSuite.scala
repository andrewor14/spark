/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection

import java.lang.{Float => JFloat}
import java.util.{Arrays, Comparator}

import org.scalatest.FunSuite

import org.apache.spark.sort.SortUtils
import org.apache.spark.sort.SortUtils.LongPairArraySorter
import org.apache.spark.util.random.XORShiftRandom

class SorterSuite extends FunSuite {

  test("TimSorter equivalent to Arrays.sort") {
    val rand = new XORShiftRandom(123)
    val data0 = Array.tabulate[Int](10000) { i => rand.nextInt() }
    val data1 = data0.clone()

    Arrays.sort(data0)
    new TimSorter(new IntArraySortDataFormat).sort(data1, 0, data1.length, Ordering.Int)

    data0.zip(data1).foreach { case (x, y) => assert(x === y) }
  }

  test("TimSorter with KVArraySorter") {
    val rand = new XORShiftRandom(456)

    // Construct an array of keys (to Java sort) and an array where the keys and values
    // alternate. Keys are random doubles, values are ordinals from 0 to length.
    val keys = Array.tabulate[Double](5000) { i => rand.nextDouble() }
    val keyValueArray = Array.tabulate[Number](10000) { i =>
      if (i % 2 == 0) keys(i / 2) else new Integer(i / 2)
    }

    // Map from generated keys to values, to verify correctness later
    val kvMap =
      keyValueArray.grouped(2).map { case Array(k, v) => k.doubleValue() -> v.intValue() }.toMap

    Arrays.sort(keys)
    new TimSorter(new KVArraySortDataFormat[Double, Number])
      .sort(keyValueArray, 0, keys.length, Ordering.Double)

    keys.zipWithIndex.foreach { case (k, i) =>
      assert(k === keyValueArray(2 * i))
      assert(kvMap(k) === keyValueArray(2 * i + 1))
    }
  }

  // NOTE: this test is highly specific to our sort format!
  test("TimSorter and RadixSorter yield same result for LongPairArraySorter") {
    val sorterFormat = new LongPairArraySorter
    val data = makeData(10, sorterFormat)
    val timOutput = data.clone()

    println("Sorting " + data.length + " Longs...")

    // Tim sort
    val timStart = System.currentTimeMillis
    new TimSorter(sorterFormat)
      .sort(timOutput, 0, data.length / 2, SortUtils.longPairOrdering)
    val timElapsed = System.currentTimeMillis - timStart

    // Radix sort
    val radixStart = System.currentTimeMillis
    val radixOutput = new RadixSorter(sorterFormat).sort(data)
    val radixElapsed = System.currentTimeMillis - radixStart

    // In case things go wrong...
    lazy val debugOutput: String = {
      "\n\n" + data.mkString(", ") + "\n\n" +
        data.map { l => l + ": " + formatLongNicely(l) }.mkString("\n")
    }

    // Note: these numbers are imprecise because
    // (1) we're sorting a small dataset,
    // (2) we're comparing these in the same JVM one after another, and
    // (3) we're only doing one iteration
    // For a more complete benchmark, see org.apache.spark.util.collection.SortMicroBenchmark
    println("TimSort took " + timElapsed + "ms")
    println("RadixSort took " + radixElapsed + "ms")
    assert(radixOutput === timOutput, "uh oh, output differed for input: " + debugOutput)
  }

  test("NOT A TEST: make sure bytes are zeroed out correctly") {
    assert(formatLongNicely(0L) ===
      "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000")
    assert(formatLongNicely(-1L) ===
      "11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111")
    assert(formatLongNicely(nullOutBytes(-1L, Array(0))) ===
      "00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111")
    assert(formatLongNicely(nullOutBytes(-1L, Array(1, 3))) ===
      "11111111 00000000 11111111 00000000 11111111 11111111 11111111 11111111")
    assert(formatLongNicely(nullOutBytes(-1L, Array(2, 4, 6))) ===
      "11111111 11111111 00000000 11111111 00000000 11111111 00000000 11111111")
    assert(formatLongNicely(nullOutBytes(-1L, Array(1, 2, 3, 4, 5, 6, 7))) ===
      "11111111 00000000 00000000 00000000 00000000 00000000 00000000 00000000")
    assert(formatLongNicely(nullOutBytes(-1L, Array(0, 1, 2, 3, 4, 5, 6, 7))) ===
      "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000")
    assert(formatLongNicely(1002398914786641992L) ===
      "00001101 11101001 00111100 10000000 10000111 00011000 01001000 01001000")
    assert(formatLongNicely(65650192293578824L) ===
      "00000000 11101001 00111100 10000000 10000111 00011000 01001000 01001000")
    assert(nullOutBytes(1002398914786641992L, Array(0)) === 65650192293578824L)
  }

  private def makeData(num: Int, s: LongPairArraySorter): Array[Long] = {
    val rand = new XORShiftRandom(123)
    Array.tabulate[Long](num) { i =>
      // Null out bytes we don't care about
      val indicesToIgnore =
        if (i % 2 == 0) {
          s.keyBytesToIgnore.filter(_ < 8)
        } else {
          s.keyBytesToIgnore.filter(_ > 8).map(_ - 8)
        }
      val l = Math.abs(rand.nextLong())
      nullOutBytes(l, indicesToIgnore.toArray)
    }
  }

  /** Zero out the n'th byte in the long. */
  private def nullOutBytes(l: Long, byteIndices: Array[Int]): Long = {
    var mask = ~0L
    byteIndices.foreach { index =>
      assume(index >= 0 && index <= 7)
      // e.g. byte index = 3,
      // mask = 11111111 11111111 11111111 00000000 11111111 11111111 11111111 11111111
      mask = mask & (~(((1L << 8) - 1) << ((7 - index) * 8)))
    }
    l & mask
  }

  /**
   * Convert a Long into a human-readable binary string.
   * e.g. 74 -> "0000000 0000000 0000000 0000000 0000000 0000000 0000000 0100101".
   */
  private def formatLongNicely(l: Long): String = {
    val binaryStr = java.lang.Long.toBinaryString(l).reverse.padTo(64, "0").reverse.mkString("")
    val res = new StringBuilder
    (0 to 7).foreach { i =>
      val start = i * 8
      val end = start + 8
      res.append(binaryStr.slice(start, end))
      res.append(" ")
    }
    res.stripSuffix(" ")toString()
  }

  /**
   * This provides a simple benchmark for comparing the TimSorter with Java internal sorting.
   * Ideally these would be executed one at a time, each in their own JVM, so their listing
   * here is mainly to have the code.
   *
   * The goal of this code is to sort an array of key-value pairs, where the array physically
   * has the keys and values alternating. The basic Java sorts work only on the keys, so the
   * real Java solution is to make Tuple2s to store the keys and values and sort an array of
   * those, while the TimSorter approach can work directly on the input data format.
   *
   * Note that the Java implementation varies tremendously between Java 6 and Java 7, when
   * the Java sort changed from merge sort to Timsort.
   */
  ignore("TimSorter benchmark") {

    /** Runs an experiment several times. */
    def runExperiment(name: String)(f: => Unit): Unit = {
      val firstTry = org.apache.spark.util.Utils.timeIt(1)(f)
      System.gc()

      var i = 0
      var next10: Long = 0
      while (i < 10) {
        val time = org.apache.spark.util.Utils.timeIt(1)(f)
        next10 += time
        println(s"$name: Took $time ms")
        i += 1
      }

      println(s"$name: ($firstTry ms first try, ${next10 / 10} ms average)")
    }

    val numElements = 25000000 // 25 mil
    val rand = new XORShiftRandom(123)

    val keys = Array.tabulate[JFloat](numElements) { i =>
      new JFloat(rand.nextFloat())
    }

    // Test our key-value pairs where each element is a Tuple2[Float, Integer)
    val kvTupleArray = Array.tabulate[AnyRef](numElements) { i =>
      (keys(i / 2): Float, i / 2: Int)
    }
    runExperiment("Tuple-sort using Arrays.sort()") {
      Arrays.sort(kvTupleArray, new Comparator[AnyRef] {
        override def compare(x: AnyRef, y: AnyRef): Int =
          Ordering.Float.compare(x.asInstanceOf[(Float, _)]._1, y.asInstanceOf[(Float, _)]._1)
      })
    }

    // Test our TimSorter where each element alternates between Float and Integer, non-primitive
    val keyValueArray = Array.tabulate[AnyRef](numElements * 2) { i =>
      if (i % 2 == 0) keys(i / 2) else new Integer(i / 2)
    }
    val sorter = new TimSorter(new KVArraySortDataFormat[JFloat, AnyRef])
    runExperiment("KV-sort using TimSorter") {
      sorter.sort(keyValueArray, 0, keys.length, new Comparator[JFloat] {
        override def compare(x: JFloat, y: JFloat): Int = Ordering.Float.compare(x, y)
      })
    }

    // Test non-primitive sort on float array
    runExperiment("Java Arrays.sort()") {
      Arrays.sort(keys, new Comparator[JFloat] {
        override def compare(x: JFloat, y: JFloat): Int = Ordering.Float.compare(x, y)
      })
    }

    // Test primitive sort on float array
    val primitiveKeys = Array.tabulate[Float](numElements) { i => rand.nextFloat() }
    runExperiment("Java Arrays.sort() on primitive keys") {
      Arrays.sort(primitiveKeys)
    }
  }
}


/** Format to sort a simple Array[Int]. Could be easily generified and specialized. */
class IntArraySortDataFormat extends SortDataFormat[Int, Array[Int]] {
  override protected def getKey(data: Array[Int], pos: Int): Int = {
    data(pos)
  }

  override protected def getLength(data: Array[Int]): Int = data.length

  override protected def swap(data: Array[Int], pos0: Int, pos1: Int): Unit = {
    val tmp = data(pos0)
    data(pos0) = data(pos1)
    data(pos1) = tmp
  }

  override protected def copyElement(src: Array[Int], srcPos: Int, dst: Array[Int], dstPos: Int) {
    dst(dstPos) = src(srcPos)
  }

  /** Copy a range of elements starting at src(srcPos) to dest, starting at destPos. */
  override protected def copyRange(src: Array[Int], srcPos: Int,
                                   dst: Array[Int], dstPos: Int, length: Int) {
    System.arraycopy(src, srcPos, dst, dstPos, length)
  }

  /** Allocates a new structure that can hold up to 'length' elements. */
  override protected def allocate(length: Int): Array[Int] = {
    new Array[Int](length)
  }
}
