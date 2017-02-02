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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._

class Job(id: Long, pn: String, sis: Seq[Int]) {
  var jobId : Long = id
  var duration : Long = 0
  var finTime : Long = 0
  var poolName : String = pn
  var stageIds = sis
}

class BatchWindow() {
  var finTime: Long = _
  var jobs: ArrayBuffer[Job] = new ArrayBuffer[Job]
  var accuracy: Double = _
  var dAccuracy: Double = _
  var totalDuration: Long = 0L
}

// Main API
object PoolReweighter extends Logging {

  type ValidationFunc = (RDD[_], Object) => Double
  type UtilityFunc = (Long, Double) => Double

  private[PoolReweighter] val validationSets = new ConcurrentHashMap[String, RDD[_]]
  private[PoolReweighter] val validationFuncs =
    new ConcurrentHashMap[String, ValidationFunc]
  private[PoolReweighter] val utilityFuncs =
    new ConcurrentHashMap[String, UtilityFunc]
  private[PoolReweighter] val models = new ConcurrentHashMap[String, Object]
  // val jobIdToJob = new ConcurrentHashMap[Long, Job]
  private[PoolReweighter] val startTime = new ConcurrentHashMap[String, Long]
  val batchWindows = new ConcurrentHashMap[String, ArrayBuffer[BatchWindow]]
  val currWindow = new ConcurrentHashMap[String, BatchWindow]
  val listener = new JobListener
  var batchTime = 0
  var isRunning = false

  def updateModel(model: Object): Unit = {
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    models.put(poolName, model)
  }

  // register your validation set with the thread you're on for testing
  def registerValidationSet(poolName: String, rdd: RDD[_]): Unit = {
    validationSets.put(poolName, rdd)
  }

  // set batch to every t seconds
  def start(t: Int = 3): Unit = {
    batchTime = t
    isRunning = true
    val thread = new Thread {
      override def run(): Unit = {
        while (isRunning) {
          Thread.sleep(1000L * t)
          batchUpdate()
        }
      }
    }
    thread.start()
  }

  // register validation function
  def registerValidationFunction(poolName: String, func: ValidationFunc): Unit = {
    validationFuncs.put(poolName, func)
  }

  def registerUtilityFunction(poolName: String, func: UtilityFunc): Unit = {
    utilityFuncs.put(poolName, func)
  }

  // register your rdd and how often you want to batch
  def register(poolName: String, rdd: RDD[_],
               valFunc: ValidationFunc,
               utilFunc: UtilityFunc): Unit = {
    registerValidationSet(poolName, rdd)
    registerValidationFunction(poolName, valFunc)
    registerUtilityFunction(poolName, utilFunc)
    SparkContext.getOrCreate.addSparkListener(listener)
  }

  def start(poolName: String): Unit = {
    startTime.put(poolName, System.currentTimeMillis())
  }

  def kill(): Unit = {
    isRunning = false
  }

  def computeAccuracy(poolName: String): Double = {
    if (models.containsKey(poolName)) {
      val accuracy = validationFuncs.get(poolName)(
        validationSets.get(poolName),
        models.get(poolName))
      accuracy
    } else 0.5
  }

  private[PoolReweighter] def batchUpdate(): Unit = {
    val numCores = SparkContext.getOrCreate().defaultParallelism
    def diff(t: (String, Double)) = t._2
    val heap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))
    val pool2numCores = new ConcurrentHashMap[String, Int]
    val currTime = System.currentTimeMillis()

    for((poolName: String, bw: BatchWindow) <- currWindow.asScala) {
      if (!batchWindows.containsKey(poolName)) {
        batchWindows.put(poolName, new ArrayBuffer[BatchWindow])
      }
      batchWindows.get(poolName).append(bw)
    }
    currWindow.clear()

    for ((poolName: String, valFunc: ValidationFunc) <- validationFuncs.asScala) {
      pool2numCores.put(poolName, 0)
      val bws = batchWindows.get(poolName)
      if(bws != null && bws.size != 0) {
        val utilFunc = utilityFuncs.get(poolName)
        val accuracy = computeAccuracy(poolName)
        bws.last.accuracy = accuracy
        // set dAccuracy
        if (bws.size > 1) {
          bws.last.dAccuracy = bws.last.accuracy - bws(bws.size - 2).accuracy
        } else {
          bws.last.dAccuracy = bws.last.accuracy - 0.5
        }
        val wallTime = currTime - startTime.get(poolName)
        val predicted = predAccuracy(poolName, 1)
        val utility = utilFunc(wallTime, predicted)
        heap.enqueue((poolName, utility))
        val weight = SparkContext.getOrCreate.getPoolWeight(poolName)
        logInfo(s"LOGAN: predicted acc of $poolName: ${predAccuracy(poolName, weight)}, " +
          s"actual acc: $accuracy")
      }
    }


    for (i <- 1 to numCores) {
      val top = heap.dequeue()
      val poolName = top._1
      val numCores = pool2numCores.get(poolName)
      pool2numCores.put(poolName, numCores + 1)
      val utilFunc = utilityFuncs.get(poolName)
      val wallTime = currTime - startTime.get(poolName)
      val predicted = predAccuracy(poolName, numCores + 1)
      val utility = utilFunc(wallTime, predicted)
      heap.enqueue((poolName, utility))
    }

    // apply new values to pool weights
    var weights = ""
    for((poolName: String, v: Int) <- pool2numCores.asScala) {
      // SparkContext.getOrCreate.setPoolWeight(poolName, v)
      weights += poolName + "," + v + " "
    }
    // logInfo(s"LOGAN changing weights: $weights")

  }

  def predAccuracy(poolName: String, numCores: Int): Double = {

    val numMs = numCores * batchTime * 1000
    val avgLen = listener.getAvgJobLen(poolName)
    // number of jobs that can be completed this round with numCores
    val numJobs = numMs / avgLen

    var sum = 0.0
    val bws = batchWindows.get(poolName)
    val lnbws = bws.takeRight(3)
    for(i <- 0 to lnbws.size - 1) {
      sum += lnbws(i).dAccuracy / lnbws(i).jobs.size
    }
    sum /= lnbws.size
//    logInfo(s"LOGAN: ${bws.last.accuracy} ${bws.last.dAccuracy}")
//    logInfo(s"LOGAN: ${bws.size} ${bws.last.jobs.size} predicting accuracy of " +
//      s"${bws.last.accuracy + sum * numJobs} for $poolName with $numCores cores")
    Math.min(bws.last.accuracy + sum * numJobs, 1.0)
  }
}


class JobListener extends SparkListener with Logging {

  val stageIdToDuration = new mutable.HashMap[Long, Long]
  val currentJobs = new mutable.HashMap[Long, Job]
  val avgJobLen = new mutable.HashMap[String, (Int, Double)]

  def getAvgJobLen(poolName: String): Double = {
    avgJobLen(poolName)._2
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val poolName = jobStart.properties.getProperty("spark.scheduler.pool")
    if(poolName != null) {
      val job = new Job(jobStart.jobId, poolName, jobStart.stageIds.toList)
      currentJobs.put(job.jobId, job)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobId = jobEnd.jobId
    if(currentJobs.contains(jobId)) {
      val job = currentJobs(jobId)
      job.finTime = jobEnd.time
      currentJobs.remove(jobId)
      if(!avgJobLen.contains(job.poolName)) {
        avgJobLen.put(job.poolName, (1, job.duration))
      } else {
        val curAvg = avgJobLen(job.poolName)
        val newAvg = (curAvg._2 * curAvg._1 + job.duration) / (curAvg._1 + 1)
        avgJobLen.put(job.poolName, (curAvg._1 + 1, newAvg))
      }
//      logInfo(s"LOGAN: $jobId in ${job.poolName} finished in ${job.duration}, " +
//        s"new avg is ${avgJobLen(job.poolName)._2} with ${avgJobLen(job.poolName)._1} elts")
      if(PoolReweighter.currWindow.containsKey(job.poolName)) {
        val bw = PoolReweighter.currWindow.get(job.poolName)
        bw.jobs.append(job)
        bw.totalDuration += job.duration
      } else {
        val bw = new BatchWindow
        bw.jobs.append(job)
        bw.totalDuration += job.duration
        PoolReweighter.currWindow.put(job.poolName, bw)
      }
    }
  }

  // figure out which job this belongs to, and add that duration
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val duration = taskEnd.taskInfo.duration
    val stageId = taskEnd.stageId
    for((jobId: Long, j: Job) <- currentJobs) {
      var contains = false
      for (i: Int <- j.stageIds) {
        if (i == stageId) {
          contains = true
        }
      }
      if (contains) j.duration += duration
    }
  }
}
