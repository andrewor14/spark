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

// scalastyle:off
package org.apache.spark.examples.mllib

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.storage.StorageLevel
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
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/kdd12")
    // val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    // val splits = data.randomSplit(Array(0.8, 0.2), seed = 11L)
    val train = data.map(s => s.features).persist(StorageLevel.MEMORY_AND_DISK)
    // val test = splits(1).cache()
    train.count()
    // test.count()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 100
    val threads = (1 to 10).map(i => new Thread {
        override def run: Unit = {
          // sc.addSchedulablePool("kmeans" + i, 0, Integer.MAX_VALUE)
          // sc.setLocalProperty("spark.scheduler.pool", "kmeans" + i)
          val clusters = KMeans.train(train, numClusters, numIterations, 1,
            KMeans.RANDOM, 1L)
          // Evaluate clustering by computing Within Set Sum of Squared Errors
          val WSSSE = clusters.computeCost(train)
          println("Within Set Sum of Squared Errors = " + WSSSE)
        }
      }).toArray
    // threads.foreach{t => t.start(); Thread.sleep(100000)}
    // threads.foreach{t => t.join() }
    threads(0).start()
    threads(0).join()
    // Save and load model
    // clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on
