package org.apache.spark.util.collection

import org.apache.commons.math3.stat.descriptive.rank.Median

import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sort.{LongPairArraySorter, SortUtils}

/**
 * A micro-benchmark to test various sorting algorithms.
 */
object SortMicroBenchmark {
  private val sortAlgorithms: Set[String] = Set("tim", "radix", "radix2")

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      sys.error(
        "Usage: SortMicroBenchmark [sort algorithm] [num records] [num iterations]\n" +
        "- sort algorithm can be one of 'tim', 'radix', or 'radix2'")
    }
    val sortAlgorithm = args(0)
    val numRecords = args(1).toInt
    val numIterations = args(2).toInt

    // Validate arguments
    assert(sortAlgorithms.contains(sortAlgorithm))
    assert(numRecords > 0)
    assert(numIterations > 0 && numIterations < 100)

    // Sort
    println(s"Sorting $numRecords records over $numIterations iteration(s)")
    val times = (1 to numIterations).map { i =>
      val rand = new XORShiftRandom(123)
      val input = Array.tabulate[Long](numRecords * 2) { _ => Math.abs(rand.nextLong()) }
      val start = System.currentTimeMillis

      SortUtils.sortLongPairs(input, sortAlgorithm)

      val elapsed = System.currentTimeMillis - start
      println(s"Iteration $i/$numIterations took ${elapsed}ms")
      elapsed
    }

    val median = new Median().evaluate(times.map(_.toDouble).toArray).toLong
    println(s"* Median over $numIterations iterations: ${median}ms")
  }
}
