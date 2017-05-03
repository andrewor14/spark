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

import breeze.stats.distributions.{Poisson, RandBasis, ThreadLocalRandomGenerator}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{PoolReweighterLoss, SparkConf, SparkContext}

object SVMVsLogisticRegression {

//  def svmValFunc(validationSet: RDD[_], m: Object): Double = {
//    val model = m.asInstanceOf[SVMModel]
//    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
//      val pred = model.predict(features)
//      (pred, label)
//    }
//    val metrics = new MulticlassMetrics(predictionsAndLabels)
//    metrics.accuracy
//  }
//
//  def logRegValFunc(validationSet: RDD[_], m: Object): Double = {
//    val model = m.asInstanceOf[LogisticRegressionModel]
//    val predictionsAndLabels = validationSet.map { case LabeledPoint(label, features) =>
//      val pred = model.predict(features)
//      (pred, label)
//    }
//    val metrics = new MulticlassMetrics(predictionsAndLabels)
//    metrics.accuracy
//  }
  // numCores is current number of cores
  def utilityFunc(time: Long, accuracy: Double): Double = {
    val quality_sens = 1.0
    val latency_sens = 1.0
    val min_qual = 0.85
    val max_latency = 60 * 1000
    quality_sens * (accuracy - min_qual) - latency_sens * (time - max_latency)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SVMVsMLPC")
      .getOrCreate()
    val sc = spark.sparkContext

    val isFair = args(0) == "fair"

    val epsilonTraining = MLUtils.loadLibSVMFile(sc, "data/mllib/epsilon")
    val mlpcData = spark.read.format("libsvm")
      .load("data/mllib/mnist8m.scale")
    val sizes = Array(0.10, 0.5, 1.0)
    // val mlpcsplits = mlpcData.randomSplit(Array(0.1, 0.1, 0.8), seed = 1234L)
    val mnistTrains = (1 to 3).map(x => mlpcData.sample(false, sizes(x % sizes.size), 42L)
                              .repartition((1000 * sizes(x % sizes.size)).toInt).cache())
    // Split data into training (98%) and validation (2%).
//    training.count() // materialize training
    val numThreads = 50

    val epsilonTrainings = (1 to 3).map(x => epsilonTraining.sample(false, sizes(x % sizes.size), 42L).cache())
// .repartition((360 * sizes(x % sizes.size)).toInt).cache())
    epsilonTrainings.foreach(x => x.count())
    mnistTrains.foreach(x => x.count())

    val sleepTime = 30 // seconds
    def setSeed(seed: Int = 0) = {
      new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    }
    val poi = new Poisson(sleepTime)(setSeed())
    val sleepTimes = poi.sample(numThreads * 2)

    val numIterations = 200
    sc.setPoolWeight("default", 32)


    val svmThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          val poolName = "svm" + i
          sc.addSchedulablePool(poolName, 0, 1000)
          sc.setLocalProperty("spark.scheduler.pool", poolName)
          PoolReweighterLoss.register(poolName, utilityFunc)
//          PoolReweighterLoss.startTime(poolName)
          val model = SVMWithSGD.train(epsilonTrainings((i - 1) % epsilonTrainings.size), numIterations, 100, 0.001, 1)
          PoolReweighterLoss.done(poolName)
        }
      }
    )

    val logRegThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          val poolName = "logistic" + i
          sc.addSchedulablePool(poolName, 0, 1000)
          sc.setLocalProperty("spark.scheduler.pool", poolName)
          PoolReweighterLoss.register(poolName, utilityFunc)
//          PoolReweighterLoss.startTime(poolName)
          val model = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(epsilonTrainings((i-1) % epsilonTrainings.size))
        }
      }
    )

    val mlpcThreads = (1 to numThreads).map( i =>
      new Thread {
        override def run: Unit = {
          val poolName = "mlpc" + i
          sc.addSchedulablePool(poolName, 0, 1000)
          sc.setLocalProperty("spark.scheduler.pool", poolName)
          PoolReweighterLoss.register(poolName, utilityFunc)
          val layers = Array[Int](784, 10, 10)
          val trainer = new MultilayerPerceptronClassifier()
            .setLayers(layers)
            .setBlockSize(128)
            .setSeed(1234L)
            .setTol(0.01)
            .setMaxIter(500)
          val model = trainer.fit(mnistTrains(( i -1 ) % mnistTrains.size))
          PoolReweighterLoss.done(poolName)
        }
      }
    )

    PoolReweighterLoss.start(3, isFair)

    (0 to numThreads - 1).foreach { i =>
      logRegThreads(i).start()
      Thread.sleep(sleepTimes(i * 2) * 1000L)
      mlpcThreads(i).start()
      Thread.sleep(sleepTimes(i * 2 + 1) * 1000L)
    }



    svmThreads.foreach { t => t.join() }
    mlpcThreads.foreach { t => t.join() }
    PoolReweighterLoss.kill()
  }
}
// scalastyle:on
