package org.apache.spark.util.collection

import org.apache.commons.math3.stat.descriptive.rank.Median

import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sort.{LongPairArraySorter, SortUtils}

/**
 * A micro-benchmark to test various sorting algorithms.
 */
object SortMicroBenchmark {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      sys.error(
        "Usage: SortMicroBenchmark [sort algorithm] [num records] [num iterations]\n" +
        "- sort algorithm can be one of 'tim' or 'radix'")
    }
    val sortAlgorithm = args(0)
    val numRecords = args(1).toInt
    val numIterations = args(2).toInt

    // Validate arguments
    assert(sortAlgorithm == "tim" || sortAlgorithm == "radix")
    assert(numRecords > 0)
    assert(numIterations > 0 && numIterations < 100)

    // Sort
    println(s"Sorting $numRecords records over $numIterations iteration(s)")
    val times = (1 to numIterations).map { i =>
      val rand = new XORShiftRandom(123)
      val input = Array.tabulate[Long](numRecords * 2) { _ => Math.abs(rand.nextLong()) }
      val start = System.currentTimeMillis

      sortAlgorithm match {
        case "tim" =>
          new TimSorter(new LongPairArraySorter)
            .sort(input, 0, numRecords, SortUtils.longPairOrdering)
        case "radix" =>
          new RadixSorter(new LongPairArraySorter).sort(input)
        case s => throw new IllegalArgumentException("unexpected sort algorithm: " + s)
      }

      val elapsed = System.currentTimeMillis - start
      println(s"Iteration $i/$numIterations took ${elapsed}ms")
      elapsed
    }

    val median = new Median().evaluate(times.map(_.toDouble).toArray).toLong
    println(s"* Median over $numIterations iterations: ${median}ms")
  }
}
