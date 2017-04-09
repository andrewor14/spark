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
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.{PoolReweighterLoss, SparkConf, SparkContext}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example for Multilayer Perceptron Classification.
 */
object MultilayerPerceptronClassifierExample {

  def utilFunc(time: Long, accuracy: Double): Double = {
    val quality_sens = 1.0
    val latency_sens = 1.0
    val min_qual = 0.85
    val max_latency = 60 * 1000
    quality_sens * (accuracy - min_qual) - latency_sens * (time - max_latency)
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MultilayerPerceptronClassifierExample")
      .config("spark.scheduler.mode", "FAIR")
      .getOrCreate()

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm")
      .load("/disk/local/disk1/andrew/data/epsilon_normalized_01label")

    // Split the data into train and test
    val splits = data.randomSplit(Array(0.9, 0.1), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val poolName = "mlpc"
    PoolReweighterLoss.startTime(poolName)
    PoolReweighterLoss.start(5)
    SparkContext.getOrCreate().addSchedulablePool(poolName, 0, 16)
    SparkContext.getOrCreate().setLocalProperty("spark.scheduler.pool", poolName)
    PoolReweighterLoss.register(poolName, utilFunc)
    val layers = Array[Int](2000, 15, 2)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(200)

    // train the model
    val model = trainer.fit(train)
    PoolReweighterLoss.kill()

    // compute accuracy on the test set
    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels))
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
