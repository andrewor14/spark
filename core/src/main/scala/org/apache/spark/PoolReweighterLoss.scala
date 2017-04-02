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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

class PRJob(id: Long, pn: String, sis: Seq[Int]) {
  var jobId : Long = id
  var duration : Long = 0
  var finTime : Long = 0
  var poolName : String = pn
  var stageIds = sis
}

class PRBatchWindow {
  var duration: Long = 0L
  var loss: Double = _
  var dLoss: Double = _
  var numCores: Int = _
}

// Main API
object PoolReweighterLoss extends Logging {

  type UtilityFunc = (Long, Double) => Double

  private[PoolReweighterLoss] val utilityFuncs =
    new ConcurrentHashMap[String, UtilityFunc]
  private[PoolReweighterLoss] val startTime = new ConcurrentHashMap[String, Long]
  private val batchWindows = new mutable.HashMap[String, ArrayBuffer[PRBatchWindow]]
  val pool2numCores = new mutable.HashMap[String, Int]
  val listener = new PRJobListener
  var batchTime = 0
  @volatile var isRunning = false

  def updateLoss(loss: Double): Unit = {
    logInfo(s"LOGAN: curr loss: $loss")
    val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
    val bws = listener.currentWindows(poolName)
    bws.loss = loss
    if(!batchWindows.contains(poolName)) {
      batchWindows.put(poolName, new ArrayBuffer[PRBatchWindow])
    }
    val bw = batchWindows(poolName)
    bw.append(bws)
    listener.currentWindows.put(poolName, new PRBatchWindow)
    // dLoss
    if(bw.size >= 2) {
      bw.last.dLoss = bw.last.loss - bw(bw.size - 2).loss
    }
    bw.last.numCores = SparkContext.getOrCreate.getPoolWeight(poolName)
  }
  // set batch to every t seconds
  def start(t: Int = 20): Unit = {
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

  def registerUtilityFunction(poolName: String, func: UtilityFunc): Unit = {
    utilityFuncs.put(poolName, func)
  }

  // register your rdd and how often you want to batch
  def register(poolName: String,
               utilFunc: UtilityFunc): Unit = {
    registerUtilityFunction(poolName, utilFunc)
    SparkContext.getOrCreate.addSparkListener(listener)
    val numCores = SparkContext.getOrCreate().defaultParallelism
    pool2numCores.put(poolName, numCores / (pool2numCores.size + 1)) // give 1/n cores
  }

  def startTime(poolName: String): Unit = {
    startTime.put(poolName, System.currentTimeMillis())
  }

  def kill(): Unit = {
    isRunning = false
  }


  private[PoolReweighterLoss] def batchUpdate(): Unit = {
    val numCores = SparkContext.getOrCreate().defaultParallelism
    def diff(t: (String, Double)) = t._2
    var posHeap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))
    var negHeap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))
    val currTime = System.currentTimeMillis()
    // first put all jobs into both heaps
    for ((poolName: String, numCores: Int) <- pool2numCores) {
      val utilFunc = utilityFuncs.get(poolName)
      val wallTime = currTime - startTime.get(poolName)
      val currCores = pool2numCores(poolName)
      val predCurr = predLoss(poolName, currCores)
      val predPlusOne = predLoss(poolName, currCores + 1)
      val predMinusOne = predLoss(poolName, currCores - 1)
      val predCurrUtil = utilFunc(wallTime, predCurr)
      val posDiff = utilFunc(wallTime, predPlusOne) - predCurrUtil
      val negDiff = utilFunc(wallTime, predMinusOne) - predCurrUtil
      posHeap.enqueue((poolName, posDiff))
      negHeap.enqueue((poolName, negDiff))
    }
    while (pool2numCores.values.sum != numCores || posHeap.head._2 + negHeap.head._2 > 0) {
      if (pool2numCores.values.sum <= numCores) {
        val posHead = posHeap.dequeue()
        val poolName = posHead._1
        val wallTime = currTime - startTime.get(poolName)
        val newPosNumCores = pool2numCores(poolName) + 1
        pool2numCores.put(poolName, newPosNumCores)
        negHeap = negHeap.filter(o => o._1 != poolName) // remove newly dequeue'd from neg heap
        val utilFunc = utilityFuncs.get(poolName)
        val predCurr = predLoss(poolName, newPosNumCores)
        val predPlusOne = predLoss(poolName, newPosNumCores + 1)
        val posDiff = utilFunc(wallTime, predPlusOne) - utilFunc(wallTime, predCurr) // re-enqueue
        posHeap.enqueue((poolName, posDiff))
      }

      if(pool2numCores.values.sum > numCores) {
        val negHead = negHeap.dequeue()
        val poolName = negHead._1
        val wallTime = currTime - startTime.get(poolName)
        val newNegNumCores = pool2numCores(poolName) - 1
        pool2numCores.put(poolName, newNegNumCores)
        posHeap = posHeap.filter(o => o._1 != poolName) // remove newly dequeue'd from pos heap
        val utilFunc = utilityFuncs.get(poolName)
        val predCurr = predLoss(poolName, newNegNumCores)
        val predMinusOne = predLoss(poolName, newNegNumCores - 1)
        val negDiff = utilFunc(wallTime, predMinusOne) - utilFunc(wallTime, predCurr)
        negHeap.enqueue((poolName, negDiff))
      }
    }

    // actually assign weights
    val sc = SparkContext.getOrCreate()
    for ((poolName: String, numCores: Int) <- pool2numCores) {
      sc.setPoolWeight(poolName, numCores)
    }
  }

  def predLoss(poolName: String, numCores: Int): Double = {

    // val numMs = numCores * batchTime * 1000
    // val avgLen = listener.getAvgJobLen(poolName)
    // number of jobs that can be completed this round with numCores
    // val numJobs = numMs / avgLen

    val bws = batchWindows(poolName)
    val numMs = numCores * batchTime * 1000
    bws.last.loss + (bws.last.dLoss / bws.last.numCores) * numCores
  }
}

class PRJobListener extends SparkListener with Logging {

  val finishedJobs = new mutable.HashMap[String, ArrayBuffer[PRJob]]
  val currentJobs = new mutable.HashMap[Long, PRJob]
  val avgJobLen = new mutable.HashMap[String, (Int, Double)]
  val currentWindows = new mutable.HashMap[String, PRBatchWindow]


  def getAvgJobLen(poolName: String): Double = {
    avgJobLen(poolName)._2
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val poolName = jobStart.properties.getProperty("spark.scheduler.pool")
    if(poolName != null && poolName != "default") {
      val job = new PRJob(jobStart.jobId, poolName, jobStart.stageIds.toList)
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
      if(finishedJobs.contains(job.poolName)) {
        finishedJobs(job.poolName).append(job)
      } else {
        val arr = new ArrayBuffer[PRJob]
        arr.append(job)
        finishedJobs(job.poolName) = arr
      }
      if(!currentWindows.contains(job.poolName)) {
        val bw = new PRBatchWindow
        currentWindows.put(job.poolName, bw)
      }
      currentWindows(job.poolName).duration += job.duration
    }
  }

  // figure out which job this belongs to, and add that duration
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val duration = taskEnd.taskInfo.duration
    val stageId = taskEnd.stageId
    for((jobId: Long, j: PRJob) <- currentJobs) {
      if (j.stageIds.contains(stageId)) j.duration += duration
    }
  }
}
