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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{PoolReweighter, SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
// $example off$

object SVMWithSGDExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext(conf)
    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label")

    // Split data into training (90%) and test (10%).
    val splits = data.randomSplit(Array(0.9, 0.1), seed = 11L)
    val training = splits(0).cache()
    training.count()
    val validation = splits(1).cache()
    validation.count()
    val numThreads = 2
    // Run training algorithm to build the model
    val numIterations = 1000

    val threads = (1 to numThreads).map(i => new Thread {
      override def run: Unit = {
        sc.addSchedulablePool("svm" + i, 0, Integer.MAX_VALUE)
        sc.setLocalProperty("spark.scheduler.pool", "svm" + i)
        PoolReweighter.registerValidationSet(validation)
        val model = SVMWithSGD.train(training, numIterations)
        model.clearThreshold()
      }
    })
    threads.foreach { t => t.start(); Thread.sleep(100000) }
    threads.foreach { t => t.join() }

    // Compute raw scores on the test set.
    // val scoreAndLabels = test.map { point =>
    //   val score = model.predict(point.features)
    //   (score, point.label)
    // }

    // Get evaluation metrics.
    // val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    // val auROC = metrics.areaUnderROC()

    // println("Area under ROC = " + auROC)

    // Save and load model
    // model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    // val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
    // $example off$

    sc.stop()
  }


}
// scalastyle:on println
