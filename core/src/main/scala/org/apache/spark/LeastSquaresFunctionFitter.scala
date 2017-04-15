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

import scala.reflect.{classTag, ClassTag}

import org.apache.commons.math3.analysis.ParametricUnivariateFunction
import org.apache.commons.math3.fitting.{AbstractCurveFitter, WeightedObservedPoint}
import org.apache.commons.math3.fitting.leastsquares.{LeastSquaresBuilder, LeastSquaresProblem}
import org.apache.commons.math3.linear.DiagonalMatrix


/*
 * All gradients are computed using the amazing partial derivative calculator here:
 * https://www.symbolab.com/solver/partial-derivative-calculator
 */

/**
 * Function that represents 1/(ax + b).
 */
class OneOverXFunction extends ParametricUnivariateFunction {
  private def denom(x: Double, a: Double, b: Double): Double = a * x + b
  override def value(x: Double, params: Double*): Double = 1 / denom(x, params(0), params(1))
  override def gradient(x: Double, params: Double*): Array[Double] = {
    val (a, b) = (params(0), params(1))
    val denomSquared = math.pow(denom(x, a, b), 2)
    Array[Double](-1 * x / denomSquared, -1 / denomSquared)
  }
}

/**
 * Function that represents 1/(a(x**2) + bx + c).
 */
class OneOverXSquaredFunction extends ParametricUnivariateFunction {
  private def denom(x: Double, a: Double, b: Double, c: Double): Double = {
    a * math.pow(x, 2) + b * x + c
  }
  override def value(x: Double, params: Double*): Double = {
    val (a, b, c) = (params(0), params(1), params(2))
    1 / denom(x, a, b, c)
  }
  override def gradient(x: Double, params: Double*): Array[Double] = {
    val (a, b, c) = (params(0), params(1), params(2))
    val denomSquared = math.pow(denom(x, a, b, c), 2)
    Array[Double](
      -1 * math.pow(x, 2) / denomSquared,
      -1 * x / denomSquared,
      -1 / denomSquared
    )
  }
}

/**
 * Function that represents 1/(a(x**k) + b).
 */
class OneOverXToTheKFunction extends ParametricUnivariateFunction {
  private def denom(x: Double, a: Double, b: Double, k: Double): Double = {
    a * math.pow(x, k) + b
  }
  override def value(x: Double, params: Double*): Double = {
    val (a, b, k) = (params(0), params(1), params(2))
    1 / denom(x, a, b, k)
  }
  override def gradient(x: Double, params: Double*): Array[Double] = {
    val (a, b, k) = (params(0), params(1), params(2))
    val denomSquared = math.pow(denom(x, a, b, k), 2)
    Array[Double](
      -1 * math.pow(x, k) / denomSquared,
      -1 * a * math.pow(x, k) * math.log(x) / denomSquared,
      -1 / denomSquared
    )
  }
}

/**
 * Curve fitter that fits 1/(ax + b) to a series of points.
 */
class OneOverXFunctionFitter extends LeastSquaresFunctionFitter[OneOverXFunction]

/**
 * Curve fitter that fits 1/(a(x**2) + bx + c) to a series of points.
 */
class OneOverXSquaredFunctionFitter extends LeastSquaresFunctionFitter[OneOverXSquaredFunction]

/**
 * Curve fitter that fits 1/(a(x**k) + b) to a series of points.
 */
class OneOverXToTheKFunctionFitter extends LeastSquaresFunctionFitter[OneOverXToTheKFunction]

/**
 * Generic curve fitter that minimizes least squares, AKA a bunch of boiler plate crap.
 */
abstract class LeastSquaresFunctionFitter[T <: ParametricUnivariateFunction: ClassTag]
  extends AbstractCurveFitter {
  protected override def getProblem(
      points: java.util.Collection[WeightedObservedPoint]): LeastSquaresProblem = {
    val len = points.size()
    val target = new Array[Double](len)
    val weights = new Array[Double](len)
    val initialGuess = Array[Double](1.0, 1.0, 1.0)

    var i = 0
    val pointIter = points.iterator()
    while (pointIter.hasNext) {
      val point = pointIter.next()
      target(i) = point.getY
      weights(i) = point.getWeight
      i += 1
    }

    val func = classTag[T].runtimeClass.getConstructor().newInstance().asInstanceOf[T]
    val model = new AbstractCurveFitter.TheoreticalValuesFunction(func, points)

    new LeastSquaresBuilder()
      .maxEvaluations(Integer.MAX_VALUE)
      .maxIterations(Integer.MAX_VALUE)
      .start(initialGuess)
      .target(target)
      .weight(new DiagonalMatrix(weights))
      .model(model.getModelFunction, model.getModelFunctionJacobian)
      .build()
  }
}

object LeastSquaresFunctionFitter {

  /**
   * Fit a curve whose shape is defined by type parameter [[T]] to the given points.
   *
   * @tparam T type of the fitted [[ParametricUnivariateFunction]]
   * @return coefficients of fitted curve
   */
  def fit[T <: LeastSquaresFunctionFitter[_]: ClassTag](
      x: Array[Double],
      y: Array[Double]): Array[Double] = {
    assert(x.length == y.length, s"x and y have different sizes: ${x.length} != ${y.length}")
    val decayFactor = 0.95
    val points = new java.util.ArrayList[WeightedObservedPoint]
    val fitter = classTag[T].runtimeClass.getConstructor().newInstance().asInstanceOf[T]
    x.zip(y).zipWithIndex.foreach { case ((t, u), i) =>
      points.add(new WeightedObservedPoint(math.pow(decayFactor, x.length - i), t, u))
    }
    fitter.fit(points)
  }

}
