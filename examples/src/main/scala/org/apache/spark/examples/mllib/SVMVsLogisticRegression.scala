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

import org.apache.spark.PoolReweighter._
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{PoolReweighter, SparkConf, SparkContext}

object SVMVsLogisticRegression {
/*
  def svmValFunc(poolName: String, validationSet: RDD[_], m: Object): Unit = {
    val currAccuracy = PoolReweighter.getAccuracy(poolName)
    val t = PoolReweighter.getMsSinceLast(poolName)
    val model = m.asInstanceOf[SVMModel]
    val predictionAndLabels = validationSet.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val dAccuracy = metrics.accuracy - currAccuracy
    var smoothedWeight = PoolReweighter.getSmoothedWeight(poolName)
    val weight = Math.max((dAccuracy * 1000000000L / t).toInt, 1)
    smoothedWeight = if(smoothedWeight == 0) weight else (smoothedWeight * 0.8 + weight * 0.2).toInt
    PoolReweighter.putSmoothedWeight(poolName, smoothedWeight)
    SparkContext.getOrCreate.setPoolWeight(poolName, smoothedWeight)
    PoolReweighter.updateAccuracy(poolName, metrics.accuracy)
  }

  def logRegValFunc(poolName: String, validationSet: RDD[_], m: Object): Unit = {
    val currAccuracy = PoolReweighter.getAccuracy(poolName)
    val t = PoolReweighter.getMsSinceLast(poolName)
    val model = m.asInstanceOf[LogisticRegressionModel]
    val predictionAndLabels = validationSet.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val dAccuracy = metrics.accuracy - currAccuracy
    var smoothedWeight = PoolReweighter.getSmoothedWeight(poolName)
    val weight = Math.max((dAccuracy * 1000000000L / t).toInt, 1)
    smoothedWeight = if(smoothedWeight == 0) weight else (smoothedWeight * 0.8 + weight * 0.2).toInt
    PoolReweighter.putSmoothedWeight(poolName, smoothedWeight)
    SparkContext.getOrCreate.setPoolWeight(poolName, smoothedWeight)
    PoolReweighter.updateAccuracy(poolName, metrics.accuracy)
  }
*/

  def svmValFunc(validationSet: RDD[_], m: Object): Double = {
    val model = m.asInstanceOf[SVMModel]
    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
      val pred = model.predict(features)
      (pred, label)
    }
    val metrics = new MulticlassMetrics(predictionsAndLabels)
    metrics.accuracy
  }

  def logRegValFunc(validationSet: RDD[_], m: Object): Double = {
    val model = m.asInstanceOf[LogisticRegressionModel]
    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
      val pred = model.predict(features)
      (pred, label)
    }
    val metrics = new MulticlassMetrics(predictionsAndLabels)
    metrics.accuracy
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
    val sleepTime = 30 // seconds
    sc.setPoolWeight("default", 32)

    val svmThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          sc.addSchedulablePool("svm" + i, 0, 32)
          sc.setLocalProperty("spark.scheduler.pool", "svm" + i)
          PoolReweighter.register(validation, svmValFunc)
          val model = SVMWithSGD.train(training, numIterations, 100, 0.00001, 1)
        }
      }
    )

    val logRegThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          sc.addSchedulablePool("logistic" + i, 0, 32)
          sc.setLocalProperty("spark.scheduler.pool", "logistic" + i)
          PoolReweighter.register(validation, logRegValFunc)
          val model = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(training)
        }
      }
    )

    (0 to numThreads - 1).foreach { i =>
      svmThreads(i).start()
      Thread.sleep(sleepTime * 1000L)
      logRegThreads(i).start()
      Thread.sleep(sleepTime * 1000L)
    }


    PoolReweighter.start()

    svmThreads.foreach { t => t.join() }
    logRegThreads.foreach { t => t.join() }
  }
}
// scalastyle:on
