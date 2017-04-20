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

import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object BestCurveFitterEver {
  private var x: Array[Double] = _
  private var y: Array[Double] = _
  private val MAX_LOSSES_PER_WINDOW = 100

  def readData(inPath: String): Unit = {
    val lines = Source.fromFile(inPath).getLines()
    val xx = new ArrayBuffer[Double]
    val yy = new ArrayBuffer[Double]
    lines.foreach { l =>
      xx += l.split(" ")(0).toDouble
      yy += l.split(" ")(1).toDouble
    }
    x = xx.toArray
    y = yy.toArray
  }

  def doTheThing(outPath: String, numItersToPredict: Int = 5): Unit = {
    assert(x != null && y != null, "no data yet, sorry")
    val pw = new PrintWriter(new File(outPath))
    try {
      y.indices.foreach { i =>
        val iter = i + 1
        val actualLoss = y(i)
        pw.write(s"ANDREW($iter): (actual loss = $actualLoss)\n")
        if (i > 2) {
          val thisWindow = y.take(i + 1).takeRight(MAX_LOSSES_PER_WINDOW)
          val losses = PoolReweighterLoss.predLoss(thisWindow, iter, numItersToPredict)
          losses.zipWithIndex.foreach { case (loss, j) =>
            pw.write(s"ANDREW(${iter + j + 1}): (predict iter = $iter) (predicted loss = $loss)\n")
          }
        }
      }
    } finally {
      pw.close()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException("Expected input path and output path")
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val sc = SparkContext.getOrCreate()
    val numItersToPredict = sc.conf.getInt(s"${PoolReweighterLoss.CONF_PREFIX}.numIterations", 5)
    readData(inputPath)
    doTheThing(outputPath, numItersToPredict)
  }

}
