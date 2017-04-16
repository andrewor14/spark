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
import org.apache.spark.util.Utils


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

object LossPredictionStrategies {
  val AVG = "avg" // raw average
  val EWMA = "ewma" // exponentially weighted moving average
  val CF = "cf" // curve fitting
}

// Main API
object PoolReweighterLoss extends Logging {
  import LossPredictionStrategies._

  type UtilityFunc = (Long, Double) => Double

  private[PoolReweighterLoss] val utilityFuncs =
    new ConcurrentHashMap[String, UtilityFunc]
  private[PoolReweighterLoss] val startTime = new ConcurrentHashMap[String, Long]
  private val batchWindows = new mutable.HashMap[String, ArrayBuffer[PRBatchWindow]]
  private val pools = new mutable.HashSet[String]
  private val listener = new PRJobListener
  private var batchTime = 0
  @volatile private var isRunning = false
  private val MAX_NUM_LOSSES = 1000

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
    logInfo(s"ANDREW($iter): $poolName (cores = $cores), (actual loss = $loss)")
  }

  // set batch to every t seconds
  def start(t: Int = 20): Unit = {
    batchTime = t
    isRunning = true
    val startTimez = System.currentTimeMillis()
    val dummyPoolName = "wheatthins"
    val thread = new Thread {
      override def run(): Unit = {
        while (isRunning) {
          Thread.sleep(1000L * t)
          val sc = SparkContext.getOrCreate()
          val totalCores = sc.defaultParallelism
          // Add a new dummy pool to hog some resources. Do this only once.
          if (pools.size == 1) {
            sc.addSchedulablePool(dummyPoolName, 0, 1)
            makeDummyThread(sc, dummyPoolName).start()
            pools.add(dummyPoolName)
          }
          // Give each pool random weights, normalized to total number of cores
          // Note that this only makes in local cluster mode
          val weights = (1 to pools.size).map { _ => scala.util.Random.nextFloat() }
          val numCores = weights.map { w =>
            scala.math.round((w / weights.sum) * totalCores)
          }.toArray
          // This might be true because of rounding issues
          if (numCores.sum < totalCores) {
            val remainingCores = totalCores - numCores.sum
            (1 to remainingCores).foreach { _ =>
              numCores(scala.util.Random.nextInt(pools.size)) += 1
            }
          }
          assert(numCores.sum == totalCores)
          // Log stuff, like cores assignment, loss prediction etc.
          val elapsed = (System.currentTimeMillis() - startTimez) / 1000
          logInfo(s"ANDREW: batch update! Time is now $elapsed.")
          pools.zip(numCores).foreach { case (pool, cores) =>
            logInfo(s"ANDREW: assigned $cores cores to pool $pool")
            sc.setPoolWeight(pool, cores)
          }
          pools.filter(_ != dummyPoolName).foreach { poolName =>
            if (batchWindows.contains(poolName)) {
              val iter = batchWindows(poolName).size
              val cores = sc.getPoolWeight(poolName)
              logInfo(s"ANDREW(${iter + 1}): $poolName (cores = $cores), (predicted loss = " +
                s"${predLoss(poolName, cores)})")
            }
          }
        }
      }
    }
    thread.start()
  }

  private def makeDummyThread(sc: SparkContext, dummyPoolName: String): Thread = {
    new Thread {
      override def run(): Unit = {
        sc.setLocalProperty("spark.scheduler.pool", dummyPoolName)
        logInfo(s"ANDREW: Starting dummy thread in pool $dummyPoolName")
        while (true) {
          sc.parallelize(1 to 10000000, 1000)
            .map { i => (i, i) }
            .reduceByKey { _ + _ }
            .count()
        }
      }
    }
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
    val bws = batchWindows(poolName)
    val conf = SparkContext.getOrCreate().getConf
    val confPrefix = "spark.approximation.predLoss"
    val strategy = conf.get(s"$confPrefix.strategy", AVG).toLowerCase
    val lossPerCore = bws.takeRight(MAX_NUM_LOSSES).map { bw => bw.loss / bw.numCores }
    val deltas = lossPerCore.zip(lossPerCore.tail).map { case (first, second) => second - first }
    val positiveDeltas = deltas.filter(_ > 0)
    if (positiveDeltas.nonEmpty) {
      logWarning(s"Some delta losses were positive: ${positiveDeltas.mkString(", ")}")
    }
    strategy match {
      case AVG =>
        // Just take the average of of the N most recent deltas
        val windowSize = conf.getInt(s"$confPrefix.$AVG.windowSize", 1)
        val trimmedDeltas = deltas.takeRight(windowSize)
        val avgDelta = if (trimmedDeltas.nonEmpty) trimmedDeltas.sum / trimmedDeltas.size else 0
        bws.last.loss + avgDelta
      case EWMA =>
        // Take the exponentially weighted moving average of all the deltas
        val alpha = conf.getDouble(s"$confPrefix.$EWMA.alpha", 0.9)
        var avgDelta = deltas.head
        deltas.tail.foreach { d =>
          avgDelta = alpha * avgDelta + (1 - alpha) * d
        }
        bws.last.loss + avgDelta
      case CF =>
        // Use a fitted curve to predict the value of the next data point
        val decay = conf.getDouble(s"$confPrefix.$CF.decay", 0.95)
        val fitterName = conf.get(s"$confPrefix.$CF.fitterName", "OneOverXSquaredFunctionFitter")
        val fitter = Utils.classForName("org.apache.spark." + fitterName)
          .getConstructor().newInstance().asInstanceOf[LeastSquaresFunctionFitter[_]]
        val x = lossPerCore.indices.map(_.toDouble).toArray
        val y = lossPerCore.toArray
        fitter.fit(x, y, decay)
        fitter.compute(x.last + 1)
      case unknown =>
        throw new IllegalArgumentException(s"Unknown loss prediction strategy: $unknown")
    }
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
