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

package org.apache.spark.mllib

import scala.collection.concurrent.TrieMap

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.classification.{LogisticRegressionModel, SVMModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
// Main API
object PoolReweighter extends Logging {
  private[PoolReweighter] val poolValidationSets = new TrieMap[String, RDD[_]]
  private[PoolReweighter] val poolModels = new TrieMap[String, (String, Object)]
  private[PoolReweighter] val newModels = new TrieMap[String, Boolean]
  private[PoolReweighter] val currAccuracy = new TrieMap[String, Double]

  def updateModel(model: (String, Object)): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    // SparkContext.getOrCreate.setPoolWeight(poolName, (value * 1000000).toInt)
    poolModels.put(poolName, model)
    newModels.put(poolName, true)
  }

  // register your validation set with the thread you're on for testing
  def registerValidationSet(rdd: RDD[_]): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    poolValidationSets.put(poolName, rdd)
  }

  // set batch to every t seconds
  def registerThread(t: Int): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    if (poolValidationSets.contains(poolName)) {
      // create the thread
      val thread = new Thread {
        override def run(): Unit = {
          while (true) {
            SparkContext.getOrCreate.setLocalProperty("spark.scheduler.pool", poolName)
            batchUpdate(poolName)
            Thread.sleep(1000L * t)
          }
        }
      }
      thread.start()
    }
  }
  // register your rdd and how often you want to batch
  def register(rdd: RDD[_], t: Int = 5): Unit = {
    registerValidationSet(rdd)
    registerThread(t)
  }

  def getValidationSet(): RDD[_] = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    poolValidationSets.getOrElse(poolName, null)
  }


  private[PoolReweighter] def batchUpdate(poolName: String): Unit = {
    if (newModels.getOrElse(poolName, false)) {
      val (n: String, m: Object) = poolModels.getOrElse(poolName, null)
      if (n.equals("svm")) {
        val validationSet = poolValidationSets
          .getOrElse(poolName, None).asInstanceOf[RDD[LabeledPoint]]
        val model = m.asInstanceOf[SVMModel]
        newModels.put(poolName, false)
        val predictionAndLabels = validationSet.map { case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
        }
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val value = metrics.accuracy - currAccuracy.getOrElse(poolName, 0.0)
        currAccuracy.put(poolName, metrics.accuracy)
        logInfo(s"LOGAN: $poolName accuracy is now ${metrics.accuracy}")
        SparkContext.getOrCreate.setPoolWeight(poolName, Math.max((value * 1000000).toInt, 1))
      } else if (n.equals("logreg")) {
        val validationSet = poolValidationSets
          .getOrElse(poolName, None).asInstanceOf[RDD[LabeledPoint]]
        val model = m.asInstanceOf[LogisticRegressionModel]
        newModels.put(poolName, false)
        val predictionAndLabels = validationSet.map { case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
        }
        val metrics = new MulticlassMetrics(predictionAndLabels)
        // update my thread weight
        val value = metrics.accuracy - currAccuracy.getOrElse(poolName, 0.0)
        currAccuracy.put(poolName, metrics.accuracy)
        logInfo(s"LOGAN: $poolName accuracy is now ${metrics.accuracy}")
        SparkContext.getOrCreate.setPoolWeight(poolName, Math.max((value * 1000000).toInt, 1))
      } else {
        logInfo(s"LOGAN: ERROR unrecognized algorithm")
      }
    }
  }
}
