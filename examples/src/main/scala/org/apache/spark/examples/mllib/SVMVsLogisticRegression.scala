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

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{PoolReweighterLoss, SparkConf, SparkContext}

object SVMVsLogisticRegression {

//  def svmValFunc(validationSet: RDD[_], m: Object): Double = {
//    val model = m.asInstanceOf[SVMModel]
//    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
//      val pred = model.predict(features)
//      (pred, label)
//    }
//    val metrics = new MulticlassMetrics(predictionsAndLabels)
//    metrics.accuracy
//  }
//
//  def logRegValFunc(validationSet: RDD[_], m: Object): Double = {
//    val model = m.asInstanceOf[LogisticRegressionModel]
//    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
//      val pred = model.predict(features)
//      (pred, label)
//    }
//    val metrics = new MulticlassMetrics(predictionsAndLabels)
//    metrics.accuracy
//  }
  // numCores is current number of cores
  def utilityFunc(time: Long, accuracy: Double): Double = {
    val quality_sens = 1.0
    val latency_sens = 1.0
    val min_qual = 0.85
    val max_latency = 60 * 1000
    quality_sens * (accuracy - min_qual) - latency_sens * (time - max_latency)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label")
    // Split data into training (98%) and validation (2%).
    val splits = data.randomSplit(Array(0.995, 0.005), seed = 1L)
    val training = splits(0).cache()
    training.count() // materialize training
    val validation = splits(1).cache()
    validation.count() //materialize validation

    val testing = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label.t")
    val numIterations = 200
    val numThreads = 1
    val sleepTime = 0 // seconds
    sc.setPoolWeight("default", 32)


    val svmThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          sc.addSchedulablePool("svm" + i, 0, 16)
          sc.setLocalProperty("spark.scheduler.pool", "svm" + i)
          // PoolReweighter.register("svm" + i, validation, svmValFunc, utilityFunc)
          PoolReweighterLoss.register("svm" + i, utilityFunc)
          val model = SVMWithSGD.train(training, numIterations, 100, 0.00001, 1)
        }
      }
    )

    val logRegThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          sc.addSchedulablePool("logistic" + i, 0, 16)
          sc.setLocalProperty("spark.scheduler.pool", "logistic" + i)
//          PoolReweighter.register("logistic" + i, validation, logRegValFunc, utilityFunc)
          PoolReweighterLoss.register("logistic" + i, utilityFunc)
          val model = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(training)
        }
      }
    )

    (0 to numThreads - 1).foreach { i =>
      svmThreads(i).start()
      PoolReweighterLoss.startTime("svm" + i)
      Thread.sleep(sleepTime * 1000L)
      logRegThreads(i).start()
      PoolReweighterLoss.startTime("logistic" + i)
      Thread.sleep(sleepTime * 1000L)
    }


    PoolReweighterLoss.start()

    svmThreads.foreach { t => t.join() }
    logRegThreads.foreach { t => t.join() }
    PoolReweighterLoss.kill()
  }
}
// scalastyle:on
