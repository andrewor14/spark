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

package org.apache.spark

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
// Main API
object PoolReweighter extends Logging {

  type ValidationFunc = (RDD[_], Object) => Double

  private[PoolReweighter] val poolValidationSets = new ConcurrentHashMap[String, RDD[_]]
  private[PoolReweighter] val poolValidationFuncs =
    new ConcurrentHashMap[String, ValidationFunc]
  private[PoolReweighter] val poolModels = new ConcurrentHashMap[String, Object]
  private[PoolReweighter] val currAccuracy = new ConcurrentHashMap[String, Double]
  private[PoolReweighter] val timePerPool = new ConcurrentHashMap[String, Long]
  private[PoolReweighter] val newModels = new ConcurrentHashMap[String, Boolean]

  def getMsSinceLast(poolName: String): Long = {
    timePerPool.get(poolName)
    timePerPool.put(poolName, 0) // reset time total
  }
  def updateAccuracy(poolName: String, acc: Double): Unit = {
    currAccuracy.put(poolName, acc)
  }

  def updateModel(model: Object): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    poolModels.put(poolName, model)
    newModels.put(poolName, true)
  }

  // register your validation set with the thread you're on for testing
  def registerValidationSet(rdd: RDD[_]): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    poolValidationSets.put(poolName, rdd)
  }

  // set batch to every t seconds
  def start(t: Int = 10): Unit = {

    val thread = new Thread {
      override def run(): Unit = {
        while (true) {
          batchUpdate()
          Thread.sleep(1000L * t)
        }
      }
    }
    thread.start()

    /*
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    // create the thread
    val thread = new Thread {
      override def run(): Unit = {
        SparkContext.getOrCreate.setLocalProperty("spark.scheduler.pool", poolName)
        while (true) {
          batchUpdate(poolName)
          Thread.sleep(1000L * t)
        }
      }
    }
    thread.start()
    */
  }

  // register validation function
  def registerValidationFunction(func: ValidationFunc): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    poolValidationFuncs.put(poolName, func)
  }

  // register your rdd and how often you want to batch
  def register(rdd: RDD[_], func: ValidationFunc): Unit = {
    registerValidationSet(rdd)
    registerValidationFunction(func)
    // registerThread(t)
  }

  def getValidationSet(): RDD[_] = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    poolValidationSets.get(poolName)
  }

  /*
  adds total amount of core time used by one algorithm
   */
  def addPoolTime(poolName: String, duration: Long): Unit = {
    val curTime = if (timePerPool.containsKey(poolName)) timePerPool.get(poolName) else 0L
    timePerPool.put(poolName, curTime + duration)
  }

  def computeAccuracy(poolName: String): Double = {
    // val prevPool = SparkContext.getOrCreate().getLocalProperty("spark.scheduler.pool")
    // SparkContext.getOrCreate().setLocalProperty("spark.scheduler.pool", poolName)
    val accuracy = poolValidationFuncs.get(poolName)(
      poolValidationSets.get(poolName),
      poolModels.get(poolName))
    // SparkContext.getOrCreate().setLocalProperty("spark.scheduler.pool", prevPool)
    accuracy
  }

  def getAccuracyOrDefault(poolName: String, default: Double): Double = {
    if (currAccuracy.containsKey(poolName)) currAccuracy.get(poolName)
    else default
  }

  private[PoolReweighter] def batchUpdate(): Unit = {
    val numCores = SparkContext.getOrCreate().defaultParallelism
    if(timePerPool.containsKey("svm1") && timePerPool.containsKey("logistic1")) {

      val coreTime1 = timePerPool.get("svm1")
      val coreTime2 = timePerPool.get("logistic1")
      timePerPool.put("svm1", 0)
      timePerPool.put("logistic1", 0)

      val totalMs = 10 * 1000 * numCores // seconds times 1000 times numCores

      logInfo(s"LOGAN: total core time: ${coreTime1 + coreTime2} vs totalMs: $totalMs")
      val newAccuracy1 = computeAccuracy("svm1")
      val newAccuracy2 = computeAccuracy("logistic1")
      val oldAccuracy1 = getAccuracyOrDefault("svm1", 0.5)
      val oldAccuracy2 = getAccuracyOrDefault("logistic1", 0.5)
      val dAccuracy1 = Math.max(newAccuracy1 - oldAccuracy1, 0)
      val dAccuracy2 = Math.max(newAccuracy2 - oldAccuracy2, 0)
      updateAccuracy("svm1", newAccuracy1)
      updateAccuracy("logistic1", newAccuracy2)

      logInfo(s"LOGAN: svm: $dAccuracy1,$coreTime1 " +
        s"logreg: $dAccuracy2,$coreTime2")

      var maxSum = 0.0
      var bestCores1 = 1
      for (i <- 1 to numCores - 1) {
        val cores1 = i
        val cores2 = numCores - i
        val cores1Ms = totalMs / cores1
        val cores2Ms = totalMs / cores2
        val oVal1 = cores1Ms.toFloat / coreTime1 * dAccuracy1
        val oVal2 = cores2Ms.toFloat / coreTime2 * dAccuracy2
        val sum = oVal1 + oVal2
        if (sum > maxSum) {
          maxSum = sum
          bestCores1 = cores1
        }
      }
      logInfo(s"LOGAN: setting svm with $bestCores1 " +
        s"cores and logreg with ${numCores - bestCores1} cores")
      SparkContext.getOrCreate().setPoolWeight("svm1", bestCores1)
      SparkContext.getOrCreate().setPoolWeight("logistic1", numCores - bestCores1)
    }
    /*
    if (poolModels.containsKey(poolName)
      && poolValidationFuncs.containsKey(poolName)
      && poolValidationSets.containsKey(poolName)
      && newModels.get(poolName)) {
      val m = poolModels.get(poolName)
      newModels.put(poolName, false)
      val valSet = poolValidationSets.get(poolName)

      val prevAccuracy = currAccuracy.get(poolName)

      val validationFunc = poolValidationFuncs.get(poolName)
      validationFunc(poolName, valSet, m)
      val dAccuracy = currAccuracy.get(poolName) - prevAccuracy
      logInfo(s"LOGAN: $poolName dAccuracy is now ${Math.max(0, dAccuracy * 1000000)}")
    }
    */
  }
}
