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
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val data = sc.textFile("data/mllib/tmp", 1000)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    parsedData.count()

    // Cluster the data into two classes using KMeans
    val numClusters = 20
    val numIterations = 1000
    val threads = (1 to 10).map(i => new Thread {
        override def run: Unit = {
          sc.addSchedulablePool("kmeans" + i, 0, Integer.MAX_VALUE)
          sc.setLocalProperty("spark.scheduler.pool", "kmeans" + i)
          val clusters = KMeans.train(parsedData, numClusters, numIterations, 1,
            KMeans.RANDOM, 1L)
          // Evaluate clustering by computing Within Set Sum of Squared Errors
          val WSSSE = clusters.computeCost(parsedData)
          println("Within Set Sum of Squared Errors = " + WSSSE)
        }
      }).toArray
    threads.foreach{t => t.start(); Thread.sleep(100000)}
    threads.foreach{t => t.join() }
    // Save and load model
    // clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
