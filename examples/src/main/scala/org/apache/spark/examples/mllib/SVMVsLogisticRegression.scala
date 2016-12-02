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

import org.apache.spark.mllib.PoolReweighter
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object SVMVsLogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label")
    // Split data into training (98%) and validation (2%).
    val splits = data.randomSplit(Array(0.995, 0.005), seed = 11L)
    val training = splits(0).cache()
    training.count() // materialize training
    val validation = splits(1).cache()
    validation.count() //materialize validation

    val testing = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label.t")
    val numIterations = 200
    val numThreads = 5
    val sleepTime = 60 // seconds
    val batchTime = 5 // seconds

    val svmThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          sc.addSchedulablePool("svm" + i, 0, 1000000)
          sc.setLocalProperty("spark.scheduler.pool", "svm" + i)
          PoolReweighter.register(validation, batchTime)
          val model = SVMWithSGD.train(training, numIterations, 100, 0.00001, 1)
        }
      }
    )

    val logRegThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          sc.addSchedulablePool("logistic" + i, 0, 1000000)
          sc.setLocalProperty("spark.scheduler.pool", "logistic" + i)
          PoolReweighter.register(validation, batchTime)
          val model = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(training)
        }
      }
    )

    (1 to numThreads).foreach { i =>
      logRegThreads(i - 1).start()
      Thread.sleep(sleepTime * 1000L)
      svmThreads(i - 1).start()
      Thread.sleep(sleepTime * 1000L)
    }
    svmThreads.foreach { t => t.join() }
    logRegThreads.foreach { t => t.join() }
  }
}
// scalastyle:on
