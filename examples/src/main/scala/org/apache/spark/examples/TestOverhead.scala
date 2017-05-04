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

package org.apache.spark.examples

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

class PRBatchWindow {
  var duration: Long = 0L
  var loss: Double = _
  var dLoss: Double = _
  var numCores: Int = _
}

// Main API
object TestOverhead extends Logging {

  var totalCores = 500
  var numJobs = 2000
  def main(args: Array[String]): Unit = {
    totalCores = args(0).toInt
    numJobs = args(0).toInt
    for (j <- 1 to numJobs) {
      val poolName = s"pool$j"
      batchWindows.put(poolName, new ArrayBuffer[PRBatchWindow])
      pool2numCores.put(poolName, 0)
      for (k <- 1 to 1000) {
        updateLoss(poolName, 1/k.toDouble)
      }
    }
    var sum = 0L
    for (j <- 1 to 100) {
      val t1 = System.currentTimeMillis()
      batchUpdate()
      val t2 = System.currentTimeMillis()
      logInfo(s"${t2-t1}")
      sum += t2-t1
    }
  }

  private val batchWindows = new mutable.HashMap[String, ArrayBuffer[PRBatchWindow]]
  val pool2numCores = new mutable.HashMap[String, Int]

  def updateLoss(poolName: String, loss: Double): Unit = {
    val bws = new PRBatchWindow
    bws.duration = 3000
    bws.loss = loss
    val bw = batchWindows(poolName)
    bw.append(bws)
    // dLoss
    if (bw.size >= 2) {
      bw.last.dLoss = bw(bw.size - 2).loss - bw.last.loss
    }
  }

  private def batchUpdate(): Unit = {
    def diff(t: (String, Double)) = t._2
    val heap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))

    var remainingCores = totalCores
    val fairshare = totalCores / numJobs
    val minCore = 3
    // first enqueue everybody at 1
    for((poolName: String, _) <- pool2numCores) {
      if (batchWindows(poolName).size <= 1) {
        pool2numCores(poolName) = fairshare
        remainingCores -= fairshare
      } else {
        val util1 = utility(predNormDLoss(poolName, minCore))
        val util2 = utility(predNormDLoss(poolName, minCore + 1))
        heap.enqueue((poolName, util2 - util1))
        pool2numCores(poolName) = minCore
        remainingCores -= minCore
      }
    }
    while(remainingCores > 0) {
      val head = heap.dequeue()
      val poolName = head._1
      val alloc = pool2numCores(poolName) + 1
      pool2numCores(poolName) = alloc
      val utilCurr = utility(predNormDLoss(poolName, alloc))
      val utilNext = utility(predNormDLoss(poolName, alloc + 1))
        heap.enqueue((poolName, utilNext - utilCurr))
      remainingCores -= 1
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
      val numMs = 1000 * numCores
      val iterations = numMs / avgIterLen
      val pLoss = predLoss(poolName, iterations)
      val maxPerPool = batchWindows(poolName).map(x => x.dLoss).max
      (batchWindows(poolName).last.loss - pLoss) / maxPerPool
    } else {
      0.0
    }
  }

  def avgTimePerIteration(poolName: String): Double = {
    if (batchWindows(poolName).size > 1) {
      batchWindows(poolName).drop(1).map(x => x.duration).sum / batchWindows(poolName).size
    } else {
      0.0
    }
  }

  def utility(predDLoss: Double): Double = predDLoss
}
