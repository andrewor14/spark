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

package org.apache.spark.examples.mllib
// scalastyle:off
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
// scalastyle:on

object KMeansVsSVM {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext(conf)

    val kmeansdata = sc.textFile("data/mllib/tmp", 1000)
    val parsedData = kmeansdata.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    parsedData.count()

    val svmdata = MLUtils.loadLibSVMFile(sc, "data/mllib/SVM_TEST_DATA6")

    // Split data into training (90%) and test (10%).
    val splits = svmdata.randomSplit(Array(0.9, 0.1), seed = 11L)
    val training = splits(0).cache()
    training.count()

    val numClusters = 20
    val numIterations = 1000
    val kmeans = new Thread() {
      override def run(): Unit = {
        sc.addSchedulablePool("kmeans", 0, 1000000)
        sc.setLocalProperty("spark.scheduler.pool", "kmeans")
        val clusters = KMeans.train(parsedData, numClusters, numIterations, 1,
          KMeans.RANDOM, 1L)
      }
    }
    val svm = new Thread() {
      override def run(): Unit = {
        sc.addSchedulablePool("svm", 0, 100000)
        sc.setLocalProperty("spark.scheduler.pool", "svm")
        val model = SVMWithSGD.train(training, numIterations)
      }
    }
    kmeans.start()
    Thread.sleep(100000) // 100 seconds
    svm.start()
    kmeans.join()
    svm.join()
  }
}
