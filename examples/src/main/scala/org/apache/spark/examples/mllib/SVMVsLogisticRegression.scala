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
// scalastyle:off
package org.apache.spark.examples.mllib

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{PoolReweighter, SparkConf, SparkContext}

object SVMVsLogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc = new SparkContext(conf)

    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label")
    // Split data into training (98%) and validation (2%).
    val splits = data.randomSplit(Array(0.98, 0.02), seed = 11L)
    val training = splits(0).cache()
    training.count() // materialize training
    val validation = splits(1).cache()
    validation.count() //materialize validation

    val testing = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon_normalized_01label.t")
    val numIterations = 200

    //first, start up SVM
    val svmThread = new Thread {
      override def run: Unit = {
        sc.addSchedulablePool("svm", 0, 1000000)
        sc.setLocalProperty("spark.scheduler.pool", "svm")
        PoolReweighter.registerValidationSet(validation)
        val model = SVMWithSGD.train(training, numIterations, 100, 0.00001, 1)
        model.clearThreshold()
      }
    }

    val logRegThread = new Thread {
      override def run: Unit = {
        sc.addSchedulablePool("logistic", 0, 1000000)
        sc.setLocalProperty("spark.scheduler.pool", "logistic")
        PoolReweighter.registerValidationSet(validation)
        val model = new LogisticRegressionWithLBFGS()
          .setNumClasses(2)
          .run(training)
      }
    }

    logRegThread.start()
    Thread.sleep(60000) //60 seconds
    svmThread.start()
    logRegThread.join()
    svmThread.join()
  }
}
// scalastyle:on
