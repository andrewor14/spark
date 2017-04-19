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

import org.apache.commons.math3.analysis.ParametricUnivariateFunction


/*
 * All gradients are computed using the amazing partial derivative calculator here:
 * https://www.symbolab.com/solver/partial-derivative-calculator
 */

/**
 * Function that represents 1 / (ax + b) + c.
 */
class OneOverXFunction extends GenericFittingFunction(3) {

  private def denom(x: Double, a: Double, b: Double): Double = a * x + b

  protected override def computeValue(x: Double, params: Seq[Double]): Double = {
    val (a, b, c) = (params(0), params(1), params(2))
    1 / denom(x, a, b) + c
  }

  protected override def computeGradient(x: Double, params: Seq[Double]): Array[Double] = {
    val (a, b) = (params(0), params(1))
    val denomSquared = math.pow(denom(x, a, b), 2)
    Array[Double](-x / denomSquared, -1 / denomSquared, 1)
  }

}


/**
 * Function that represents 1 / (ax**2 + bx + c) + d.
 */
class OneOverXSquaredFunction extends GenericFittingFunction(4) {

  private def denom(x: Double, a: Double, b: Double, c: Double): Double = {
    a * math.pow(x, 2) + b * x + c
  }

  protected override def computeValue(x: Double, params: Seq[Double]): Double = {
    val (a, b, c, d) = (params(0), params(1), params(2), params(3))
    1 / denom(x, a, b, c) + d
  }

  protected override def computeGradient(x: Double, params: Seq[Double]): Array[Double] = {
    val (a, b, c, d) = (params(0), params(1), params(2), params(3))
    val denomSquared = math.pow(denom(x, a, b, c), 2)
    Array[Double](
      -math.pow(x, 2) / denomSquared,
      -x / denomSquared,
      -1 / denomSquared,
      1
    )
  }

}


/**
 * Function that represents 1 / (ax**k + b) + c.
 */
class OneOverXToTheKFunction extends GenericFittingFunction(4) {

  private def denom(x: Double, a: Double, b: Double, k: Double): Double = {
    a * math.pow(x, k) + b
  }

  protected override def computeValue(x: Double, params: Seq[Double]): Double = {
    val (a, b, c, k) = (params(0), params(1), params(2), params(3))
    1 / denom(x, a, b, k) + c
  }

  protected override def computeGradient(x: Double, params: Seq[Double]): Array[Double] = {
    val (a, b, c, k) = (params(0), params(1), params(2), params(3))
    val denomSquared = math.pow(denom(x, a, b, k), 2)
    Array[Double](
      -math.pow(x, k) / denomSquared, // a
      -1 / denomSquared, // b
      1, // c
      -a * math.pow(x, k) * math.log(x) / denomSquared // k
    )
  }

}


/**
 * Function that represents exp(-ax) + b.
 */
class OneOverExponentialFunction extends GenericFittingFunction(2) {

  protected override def computeValue(x: Double, params: Seq[Double]): Double = {
    val (a, b) = (params(0), params(1))
    math.exp(-a * x) + b
  }

  protected override def computeGradient(x: Double, params: Seq[Double]): Array[Double] = {
    val (a, b) = (params(0), params(1))
    Array[Double](-x * math.exp(-a * x), 1)
  }
}


/**
 * Abstract fitting function that validates number of parameters before computing
 * the value or the gradient of the function.
 */
abstract class GenericFittingFunction(val numParameters: Int)
  extends ParametricUnivariateFunction {

  private def validateNumParams(params: Seq[Double]): Unit = {
    assert(params.length == numParameters,
      s"expected $numParameters parameters, got ${params.length}: ${params.mkString(", ")}")
  }

  override def value(x: Double, params: Double*): Double = {
    validateNumParams(params)
    computeValue(x, params)
  }

  override def gradient(x: Double, params: Double*): Array[Double] = {
    validateNumParams(params)
    computeGradient(x, params)
  }

  protected def computeValue(x: Double, params: Seq[Double]): Double
  protected def computeGradient(x: Double, params: Seq[Double]): Array[Double]
}
