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
  var isFair = false
  @volatile var isRunning = false

  def updateLoss(loss: Double): Unit = {
    if (loss != 1.0) {
      val poolName = SparkContext.getOrCreate.getLocalProperty("spark.scheduler.pool")
      logInfo(s"LOGAN: $poolName curr loss: $loss")
      val bws = listener.currentWindows(poolName)
      bws.loss = loss
      val bw = batchWindows(poolName)
      bw.append(bws)
      listener.currentWindows.put(poolName, new PRBatchWindow)
      // dLoss
      if (bw.size >= 2) {
        bw.last.dLoss = bw(bw.size - 2).loss - bw.last.loss
      }
      bw.last.numCores = SparkContext.getOrCreate.getPoolWeight(poolName)
    }

  }

  def done(poolName: String): Unit = {
    pool2numCores.remove(poolName)
  }

  // set batch to every t seconds
  def start(t: Int = 10, fair: Boolean = false): Unit = {
    isFair = fair
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
    // pool2numCores.put(poolName, Math.max(numCores / (pool2numCores.size + 1), 3))
    pool2numCores.put(poolName, -1)
    batchWindows.put(poolName, new ArrayBuffer[PRBatchWindow])
    batchUpdate()
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
    val heap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))
    val currTime = System.currentTimeMillis()

    var remainingCores = numCores
    val fairshare = numCores / pool2numCores.size
    // first enqueue everybody at 1
    for((poolName: String, _) <- pool2numCores) {
      if (batchWindows(poolName).size <= 1) {
        pool2numCores(poolName) = fairshare
        remainingCores -= fairshare
      } else {
        val util1 = utility(predNormDLoss(poolName, 1))
        val util2 = utility(predNormDLoss(poolName, 2))
        heap.enqueue((poolName, util2 - util1))
        pool2numCores(poolName) = 1
        remainingCores -= 1
      }
    }

    while(remainingCores > 0) {
      val head = heap.dequeue()
//       logInfo(s"LOGAN dequeuing ${head._1}, ${head._2}")
      val poolName = head._1
      val alloc = pool2numCores(poolName) + 1
      pool2numCores(poolName) = alloc
      val utilCurr = utility(predNormDLoss(poolName, alloc))
      val utilNext = utility(predNormDLoss(poolName, alloc + 1))
      heap.enqueue((poolName, utilNext - utilCurr))
      remainingCores -= 1
    }
//    assert(pool2numCores.values.sum == numCores)

    // actually assign weights
    var weights = ""
    val sc = SparkContext.getOrCreate()
    for ((poolName: String, numCores: Int) <- pool2numCores) {
      sc.setPoolWeight(poolName, numCores)
      weights += poolName + ", " + numCores + ":"
      if (batchWindows(poolName).nonEmpty) {
        weights += batchWindows(poolName).last.loss
      } else {
        weights += "0.0"
      }
    }
    logInfo(s"LOGAN weights: $weights")

    if (isFair) {
      // fair share:
      for ((poolName: String, numCores: Int) <- pool2numCores) {
        sc.setPoolWeight(poolName, 1)
      }
    }

  }

  // predicts loss numIterations in the future (can be non-integer)
  def predLoss(poolName: String, numIterations: Double): Double = {
    val bws = batchWindows(poolName)
    if(bws.size > 1) {
      val lastLoss = bws.last.loss
      val nextLastLoss = bws(bws.size - 2).loss
      val a = lastLoss / nextLastLoss
      lastLoss * Math.pow(a, numIterations)
    } else {
      0.0
    }
  }

  // predict the normalized delta loss
  def predNormDLoss(poolName: String, numCores: Int): Double = {
    if (batchWindows(poolName).size > 1) {
      val avgIterLen = avgTimePerIteration(poolName)
      val numMs = 1000 * numCores * batchTime
      val iterations = numMs / avgIterLen
      val pLoss = predLoss(poolName, iterations)
      val maxPerPool = batchWindows(poolName).map(x => x.dLoss).max
      (batchWindows(poolName).last.loss - pLoss) / maxPerPool
    } else {
      0.0
    }
  }

  def avgTimePerIteration(poolName: String): Double = {
    if (batchWindows(poolName).nonEmpty) {
      batchWindows(poolName).map(x => x.duration).sum / batchWindows(poolName).size
    } else {
      0.0
    }
  }

  def utility(predDLoss: Double): Double = predDLoss
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
