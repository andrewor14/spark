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

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.ui.jobs.UIData.JobUIData

class Job(id: Long, pn: String) {
  var jobId : Long = id
  var duration : Long = 0
  var done : Boolean = false
  var poolName : String = pn
}

// Main API
object PoolReweighter extends Logging {

  type ValidationFunc = (RDD[_], Object) => Double
  type UtilityFunc = (Long, Double, Int) => Double

  private[PoolReweighter] val validationSets = new ConcurrentHashMap[String, RDD[_]]
  private[PoolReweighter] val validationFuncs =
    new ConcurrentHashMap[String, ValidationFunc]
  private[PoolReweighter] val utilityFuncs =
    new ConcurrentHashMap[String, UtilityFunc]
  private[PoolReweighter] val models = new ConcurrentHashMap[String, Object]
  private[PoolReweighter] val currAccuracy = new ConcurrentHashMap[String, Double]
  private[PoolReweighter] val jobIdToJob = new ConcurrentHashMap[Long, Job]
  private[PoolReweighter] val startTime = new ConcurrentHashMap[String, Long]

  def updateAccuracy(poolName: String, acc: Double): Unit = {
    currAccuracy.put(poolName, acc)
  }

  def updateModel(model: Object): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    models.put(poolName, model)
  }

  // register your validation set with the thread you're on for testing
  def registerValidationSet(rdd: RDD[_]): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    validationSets.put(poolName, rdd)
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
  }

  // register validation function
  def registerValidationFunction(func: ValidationFunc): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    validationFuncs.put(poolName, func)
  }

  def registerUtilityFunction(func: UtilityFunc): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    utilityFuncs.put(poolName, func)
  }

  // register your rdd and how often you want to batch
  def register(rdd: RDD[_], valFunc: ValidationFunc, utilFunc: UtilityFunc): Unit = {
    registerValidationSet(rdd)
    registerValidationFunction(valFunc)
    registerUtilityFunction(utilFunc)
    // registerThread(t)
  }

  def start(poolName: String): Unit = {
    startTime.put(poolName, System.currentTimeMillis())
  }

  def getValidationSet(): RDD[_] = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    validationSets.get(poolName)
  }

  def mapJobToPool(jobId: Long, poolName: String): Unit = {
    if (!jobIdToJob.containsKey(jobId)) {
      jobIdToJob.put(jobId, new Job(jobId, poolName))
    } else {
      jobIdToJob.get(jobId).poolName = poolName
    }
  }

  def jobEnd(jobData: JobUIData): Unit = {
    jobIdToJob.get(jobData.jobId).done = true
  }

  /*
  adds total amount of core time used by one algorithm
   */
  def addPoolTime(jobId: Long, duration: Long): Unit = {
    val job = jobIdToJob.get(jobId)
    job.duration += duration
  }

  def computeAccuracy(poolName: String): Double = {
    val accuracy = validationFuncs.get(poolName)(
      validationSets.get(poolName),
      models.get(poolName))
    accuracy
  }

  private[PoolReweighter] def batchUpdate(): Unit = {
    val numCores = SparkContext.getOrCreate().defaultParallelism
    def diff(t: (String, Double)) = t._2
    val heap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))
    val pool2numCores = new ConcurrentHashMap[String, Int]
    val dAccuracy = new ConcurrentHashMap[String, Double]
    val currTime = System.currentTimeMillis()

    for((jobId: Long, job: Job) <- jobIdToJob.asScala) {
      pool2numCores.put(job.poolName, 0)
      val utilFunc = utilityFuncs.get(job.poolName)
      val newAccuracy = computeAccuracy(job.poolName)
      val oldAccuracy = currAccuracy.getOrDefault(job.poolName, 0.5)
      dAccuracy.put(job.poolName, newAccuracy - oldAccuracy)
      updateAccuracy(job.poolName, newAccuracy)

      val wallTime = currTime - startTime.get(job.poolName)
      val utility = utilFunc(wallTime, predAccuracy(newAccuracy, dAccuracy.get(job.poolName)), 0)
      heap.enqueue((job.poolName, utility))
    }

    for(i <- 1 to numCores) {
      val top = heap.dequeue()
      val poolName = top._1
      val numCores = pool2numCores.get(poolName)
      pool2numCores.put(poolName, numCores + 1)
      val utilFunc = utilityFuncs.get(poolName)
      val wallTime = currTime - startTime.get(poolName)
      val utility = utilFunc(wallTime, currAccuracy.get(poolName), numCores)
      heap.enqueue((poolName, utility))
    }

    // apply new values to pool weights
    var weights = ""
    for((poolName: String, v: Int) <- pool2numCores.asScala) {
      SparkContext.getOrCreate.setPoolWeight(poolName, v)
      weights += poolName + "," + v + " "
    }
    logInfo(s"LOGAN changing weights: $weights")
  }

  def predAccuracy(currAccuracy: Double, dAccuracy: Double): Double = {
    currAccuracy + dAccuracy
  }

}
