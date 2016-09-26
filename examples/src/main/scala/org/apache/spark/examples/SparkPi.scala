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

// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    val sc = new SparkContext()
    sc.addSchedulablePool("sparkpi1",1,1)
    sc.addSchedulablePool("sparkpi2",1,1)
    val t1 = new Thread{
        override def run() {
          sc.setLocalProperty("spark.scheduler.pool","sparkpi1")
          val slices = if (args.length > 0) args(0).toInt else 2
          var countTotal = 0
          var piApprox = 0
          for(i <- 1 to 1000) {
            val startTime = System.currentTimeMillis()
            val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
            val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
              val x = random * 2 - 1
              val y = random * 2 - 1
              if (x*x + y*y < 1) 1 else 0
            }.reduce(_ + _)
          countTotal += count
          val newPi = 4.0 * countTotal / ((n - 1) * i)
          val dPi = Math.abs(dPi - piApprox)
          piApprox = newPi
          val t = System.currentTimeMillis() - startTime
          val newWeight = dPi / t
          sc.setPoolWeight("sparkpi1",newWeight)
          }
        }
        println(s"Pi1 is approximately $piApprox")
    }.start()
    val t2 = new Thread{
        override def run() {
          sc.setLocalProperty("spark.scheduler.pool","sparkpi2")
          val slices = if (args.length > 0) args(0).toInt else 2
          var countTotal = 0
          var piApprox = 0
          for(i <- 1 to 1000) {
            val startTime = System.currentTimeMillis()
            val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
            val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
              val x = random * 2 - 1
              val y = random * 2 - 1
              if (x*x + y*y < 1) 1 else 0
            }.reduce(_ + _)
          countTotal += count
          val newPi = 4.0 * countTotal / ((n - 1) * i)
          val dPi = Math.abs(dPi - piApprox)
          piApprox = newPi
          val t = System.currentTimeMillis() - startTime
          val newWeight = dPi / t
          sc.setPoolWeight("sparkpi2",newWeight)
          }
        }
        println(s"Pi2 is approximately $piApprox")
    }.start()
    
    spark.stop()
  }
}
// scalastyle:on println
