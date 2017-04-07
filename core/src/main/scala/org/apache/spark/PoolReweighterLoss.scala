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
  private val pools = new mutable.HashSet[String]
  private val listener = new PRJobListener
  private var batchTime = 0
  @volatile private var isRunning = false

  def updateLoss(loss: Double): Unit = {
    val poolName = SparkContext.getOrCreate().getLocalProperty("spark.scheduler.pool")
    val bw = listener.currentWindows(poolName)
    bw.loss = loss
    if(!batchWindows.contains(poolName)) {
      batchWindows.put(poolName, new ArrayBuffer[PRBatchWindow])
    }
    val bws = batchWindows(poolName)
    bws.append(bw)
    listener.currentWindows.put(poolName, new PRBatchWindow)
    // dLoss
    if(bws.size >= 2) {
      bws.last.dLoss = bws.last.loss - bws(bws.size - 2).loss
    }
    bws.last.numCores = SparkContext.getOrCreate().getPoolWeight(poolName)
    val iter = bws.size
    val cores = bws.last.numCores
    logInfo(s"Them num cores for them pool $poolName is $cores")
    logInfo(s"ANDREW($iter): $poolName actual loss = $loss")
    logInfo(s"ANDREW(${iter + 1}): $poolName predicted loss using $cores cores " +
      s"= ${predLoss(poolName, cores)}")
  }
  // set batch to every t seconds
  def start(t: Int = 20): Unit = {
    batchTime = t
    isRunning = true
    val thread = new Thread {
      override def run(): Unit = {
        while (isRunning) {
          Thread.sleep(1000L * t)
          val sc = SparkContext.getOrCreate()
          val totalCores = sc.defaultParallelism
          if (pools.size == 1) {
            // Pretend there are other applications running for now
            val weight = scala.math.round(scala.util.Random.nextFloat() * totalCores)
            sc.setPoolWeight(pools.head, weight)
          } else if (pools.size > 1) {
            // Give each pool random weights, normalized to total number of cores
            // Note that this only makes in local cluster mode
            val numCores = new ArrayBuffer[Int](pools.size)
            var i = 0
            while (numCores.sum < totalCores) {
              if (scala.util.Random.nextFloat() > 0.5) {
                numCores(i % totalCores) += 1
              }
              i += 1
            }
            assert(numCores.sum == totalCores)
            pools.zip(numCores).foreach { case (pool, cores) => sc.setPoolWeight(pool, cores) }
          }
        }
      }
    }
    thread.start()
  }

  def registerUtilityFunction(poolName: String, func: UtilityFunc): Unit = {
    utilityFuncs.put(poolName, func)
  }

  // register your rdd and how often you want to batch
  def register(
      poolName: String,
      utilFunc: UtilityFunc): Unit = {
    registerUtilityFunction(poolName, utilFunc)
    SparkContext.getOrCreate().addSparkListener(listener)
    pools.add(poolName)
  }

  def startTime(poolName: String): Unit = {
    startTime.put(poolName, System.currentTimeMillis())
  }

  def kill(): Unit = {
    isRunning = false
  }

  private def batchUpdate(): Unit = {
    val numCores = SparkContext.getOrCreate().defaultParallelism
    def diff(t: (String, Double)) = t._2
    val heap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))
    val pool2numCores = new ConcurrentHashMap[String, Int]
    val currTime = System.currentTimeMillis()
//    if (heap.nonEmpty) {
//      for (i <- 1 to numCores) {
//        val top = heap.dequeue()
//        val poolName = top._1
//        val numCores = pool2numCores.get(poolName)
//        pool2numCores.put(poolName, numCores + 1)
//        val utilFunc = utilityFuncs.get(poolName)
//        val wallTime = currTime - startTime.get(poolName)
//        val predicted = predLoss(poolName, numCores + 1)
//        val utility = utilFunc(wallTime, predicted)
//        heap.enqueue((poolName, utility))
//      }
//      var weights = ""
//      for ((poolName: String, v: Int) <- pool2numCores.asScala) {
//        weights += poolName + "," + v + " "
//      }
//      logInfo(s"LOGAN: changing weights: $weights")
//    }
    // TO HERE NEEDS FIXING
  }

  private def predLoss(poolName: String, numCores: Int): Double = {

    // val numMs = numCores * batchTime * 1000
    // val avgLen = listener.getAvgJobLen(poolName)
    // number of jobs that can be completed this round with numCores
    // val numJobs = numMs / avgLen

    val bws = batchWindows(poolName)
    // Take the average of the N most recent deltas
    val deltas = bws.map { bw => (bw.dLoss / bw.numCores) * numCores }.takeRight(1)
    val averageDelta = deltas.sum / deltas.size
    bws.last.loss + averageDelta
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
