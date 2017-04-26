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
// scalastyle:off
// $example on$

import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.{PoolReweighterLoss, SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
// $example off$

object TestExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val spark = SparkSession
      .builder
      .getOrCreate()
    // $example on$
    // Load training data in LIBSVM format.
    var data = MLUtils.loadLibSVMFile(spark.sparkContext, "data/mllib/sample_lda_libsvm_data.txt")

    val df = spark.createDataFrame(data.map(x => (x.label, x.features))).toDF("label", "features")
    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(15)
    val polyDF = polynomialExpansion.transform(df)
    data = polyDF.rdd.map(row => new LabeledPoint(row(0).asInstanceOf[Double], row(2).asInstanceOf[Vector]))
    data.collect.foreach(println)
    spark.sparkContext.stop()
  }


}
// scalastyle:on println
