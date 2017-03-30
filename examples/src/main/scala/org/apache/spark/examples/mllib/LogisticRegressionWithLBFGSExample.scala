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
// $example on$

import org.apache.spark.rdd.RDD
import org.apache.spark.{PoolReweighterLoss, SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.storage.StorageLevel
// $example off$

object LogisticRegressionWithLBFGSExample {

  def utilFunc(time: Long, accuracy: Double): Double = {
    val quality_sens = 1.0
    val latency_sens = 1.0
    val min_qual = 0.85
    val max_latency = 60 * 1000
    quality_sens * (accuracy - min_qual) - latency_sens * (time - max_latency)
  }

  def valFunc(validationSet: RDD[_], m: Object): Double = {
    val model = m.asInstanceOf[LogisticRegressionModel]
    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
      val pred = model.predict(features)
      (pred, label)
    }
    val metrics = new MulticlassMetrics(predictionsAndLabels)
    metrics.accuracy
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionWithLBFGSExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/mnist8m.scale")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.95, 0.05), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1).cache()
    val poolName = "logistic"
    training.count()
    validation.count()
//    val test = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label.t")

     sc.addSchedulablePool(poolName, 0, 32)
     sc.setLocalProperty("spark.scheduler.pool", poolName)
    // PoolReweighter.register("logistic", validation, valFunc, utilFunc)
    // PoolReweighter.startTime("logistic")
    // PoolReweighter.start(5)
    // Run training algorithm to build the model
//    val model = new LogisticRegressionWithSGD().run(training)
    PoolReweighterLoss.register(poolName, utilFunc)
    PoolReweighterLoss.startTime(poolName)
    PoolReweighterLoss.start(10)
    val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(10)
      .run(training)
    PoolReweighterLoss.kill()
    // PoolReweighter.kill()
    // Compute raw scores on the test set.
    val predictionAndLabels = validation.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
//
//    // Save and load model
//    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
//    val sameModel = LogisticRegressionModel.load(sc,
//      "target/tmp/scalaLogisticRegressionWithLBFGSModel")
//    // $example off$

    sc.stop()
  }
}
// scalastyle:on
