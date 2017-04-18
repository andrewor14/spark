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

  private val CONF_PREFIX = "spark.approximation.predLoss"

  def updateLoss(loss: Double): Unit = listener.synchronized {
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
          listener.synchronized {
            pools.filter(_ != dummyPoolName).foreach { poolName =>
              if (batchWindows.contains(poolName)) {
                val iter = batchWindows(poolName).size
                val cores = sc.getPoolWeight(poolName)
                val numItersToPredict = sc.conf.getInt(s"$CONF_PREFIX.numIterations", 5)
                val predictedLosses = predLoss(poolName, numItersToPredict)
                predictedLosses.zipWithIndex.foreach { case (loss, i) =>
                  logInfo(s"ANDREW(${iter + i}): $poolName " +
                    s"(cores = $cores), " +
                    s"(predict iter = $iter), " +
                    s"(predicted loss = $loss)")
                }
              }
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

  private def predLoss(poolName: String, numItersToPredict: Int): Array[Double] = {
    val bws = batchWindows(poolName)
    val conf = SparkContext.getOrCreate().getConf
    val strategy = conf.get(s"$CONF_PREFIX.strategy", AVG).toLowerCase
    val windowSize = conf.getInt(s"$CONF_PREFIX.windowSize", 100)
    val lossWithIndex =
      bws.zipWithIndex.map { case (bw, i) => (i, bw.loss) }.takeRight(windowSize).toArray
    val (lossIndices, losses) = lossWithIndex.unzip
    if (losses.length <= 1) {
      return Array.empty[Double]
    }
    val deltas = losses.zip(losses.tail).map { case (first, second) => second - first }
    val positiveDeltas = deltas.filter(_ > 0)
    if (positiveDeltas.nonEmpty) {
      logWarning(s"Some delta losses were positive: ${positiveDeltas.mkString(", ")}")
    }
    val predictedLosses = strategy match {
      case AVG =>
        val avgDelta = if (deltas.nonEmpty) deltas.sum / deltas.length else 0
        (1 to numItersToPredict).map { i => bws.last.loss + i * avgDelta }.toArray
      case EWMA =>
        // Take the exponentially weighted moving average of all the deltas
        val alpha = conf.getDouble(s"$CONF_PREFIX.$EWMA.alpha", 0.9)
        var avgDelta = deltas.head
        deltas.tail.foreach { d =>
          avgDelta = alpha * avgDelta + (1 - alpha) * d
        }
        (1 to numItersToPredict).map { i => bws.last.loss + i * avgDelta }.toArray
      case CF =>
        // Use a fitted curve to predict the value of the next data point
        val decay = conf.getDouble(s"$CONF_PREFIX.$CF.decay", 0.95)
        val fitterName = conf.get(s"$CONF_PREFIX.$CF.fitterName", "OneOverXSquaredFunctionFitter")
        val fitter = Utils.classForName("org.apache.spark." + fitterName)
          .getConstructor().newInstance().asInstanceOf[LeastSquaresFunctionFitter[_]]
        val x = lossIndices.map(_.toDouble)
        val y = losses
        val currentIter = x.last + 1
        fitter.fit(x, y, decay)
        logInfo("\n\n\n==========================================")
        logInfo(s"ANDREW predicting in iteration $currentIter")
        logInfo(s"My x's are: ${x.mkString(", ")}.")
        logInfo(s"My y's are: ${y.mkString(", ")}.")
        logInfo(s"My params are ${fitter.getFittedParams.mkString(" ")}")
        logInfo("My predictions for iterations " +
          s"${(1 to numItersToPredict).map(_ + currentIter).mkString(", ")} are: " +
          s"${(1 to numItersToPredict).map { i => fitter.compute(currentIter + i)}.mkString(", ")}")
        logInfo("==========================================")
        (1 to numItersToPredict).map { i => fitter.compute(currentIter + i)}.toArray
      case unknown =>
        throw new IllegalArgumentException(s"Unknown loss prediction strategy: $unknown")
    }
    predictedLosses.map { x => if (x < 0) 0 else x }
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
