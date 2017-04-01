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
// scalastyle:off
// $example on$

import org.apache.spark.mllib.classification.MulticlassSVMModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{PoolReweighterLoss, SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.classification.MulticlassSVMWithSGD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
// $example off$

object SVMWithSGDExample {

  def utilFunc(time: Long, accuracy: Double): Double = {
    val quality_sens = 1.0
    val latency_sens = 1.0
    val min_qual = 0.85
    val max_latency = 60 * 1000
    quality_sens * (accuracy - min_qual) - latency_sens * (time - max_latency)
  }
//
//  def valFunc(validationSet: RDD[_], m: Object): Double = {
//    val model = m.asInstanceOf[MulticlassSVMModel]
//    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
//      val pred = model.predict(features)
//      (pred, label)
//    }
//    val metrics = new MulticlassMetrics(predictionsAndLabels)
//    metrics.accuracy
//  }



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext(conf)
    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/mnist8m.scale")

    // Split data into training (90%) and test (10%).
    val splits = data.randomSplit(Array(0.8, 0.02, 0.18), seed = 11L)
    val training = splits(0).cache()
    training.count()
    val validation = splits(1).cache()
    validation.count()
    val test = splits(1).cache()
    test.count()
    val numIterations = 100
    val stepSize = 500 // 500
    val regParam = 0.0001 // 0.0001
    val miniBatchFraction = 1.0
    val poolName = "svm"
    // val model = SVMWithSGD.train(training, numIterations)
    sc.addSchedulablePool(poolName, 0, 32)
    sc.setLocalProperty("spark.scheduler.pool", poolName)
    PoolReweighterLoss.start(10)
    PoolReweighterLoss.startTime(poolName)
    PoolReweighterLoss.register(poolName, utilFunc)
    val model = MulticlassSVMWithSGD.train(training, numIterations,
      stepSize, regParam, miniBatchFraction, 10)
    PoolReweighterLoss.kill()
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    val metrics = new MulticlassMetrics(scoreAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")



    // val svmAlg = new MulticlassSVMWithSGD()
    // svmAlg.optimizer
//      .setNumIterations(200)
//      .setRegParam(0.00001)
//      .setStepSize(100)
//      .setConvergenceTol(0.000001)
//      .setUpdater(new L1Updater)
    // val model = svmAlg.run(training)

//    model.clearThreshold()

//    println("Model weights = ")
//    println(model.weights)

    /*
    val numThreads = 2
    // Run training algorithm to build the model
    val numIterations = 1000

    val threads = (1 to numThreads).map(i => new Thread {
      override def run: Unit = {
        sc.addSchedulablePool("svm" + i, 0, Integer.MAX_VALUE)
        sc.setLocalProperty("spark.scheduler.pool", "svm" + i)
        PoolReweighter.registerValidationSet(validation)
        val model = SVMWithSGD.train(training, numIterations)
        model.clearThreshold()
      }
    })
    threads.foreach { t => t.start(); Thread.sleep(100000) }
    threads.foreach { t => t.join() }
    */

    // Compute raw scores on the test set.
    /*
    val test = validation
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    println("Begin printing results")
    scoreAndLabels.collect().foreach(println)
    println("End printing results")

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    model.setThreshold(0)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics2 = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics2.accuracy
    println(s"Accuracy = $accuracy")
    */

    // Save and load model
    // model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    // val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
    // $example off$

    sc.stop()
  }


}
// scalastyle:on println
