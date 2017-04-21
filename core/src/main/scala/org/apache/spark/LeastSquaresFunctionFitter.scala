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


class OneOverXFunctionFitter extends LeastSquaresFunctionFitter[OneOverXFunction]
class OneOverXSquaredFunctionFitter extends LeastSquaresFunctionFitter[OneOverXSquaredFunction]


/**
 * Generic curve fitter that minimizes least squares.
 */
abstract class LeastSquaresFunctionFitter[T <: GenericFittingFunction: ClassTag] {
  private var fittedParams: Array[Double] = _
  private val func: T = classTag[T].runtimeClass.getConstructor().newInstance().asInstanceOf[T]

  /**
   * Return the fitted parameters, assuming [[fit]] has already been called.
   */
  def getFittedParams: Array[Double] = {
    if (fittedParams != null) {
      fittedParams
    } else {
      throw new IllegalStateException("parameters have not been fitted yet; call `fit` first")
    }
  }

  /**
   * Compute the value of the fitted function at `x`, assuming [[fit]] has already been called.
   */
  def compute(x: Double): Double = {
    func.compute(x, getFittedParams: _*)
  }

  /**
   * Fit this function fitter to the given points.
   *
   * The caller may optionally specify a decay in (0, 1] to give exponentially less
   * weight to early data points.
   */
  def fit(
      x: Array[Double],
      y: Array[Double],
      decay: Double = 0.9,
      startingParameters: Array[Double] = Array.empty[Double]): Unit = {
    assert(x.length == y.length, s"x and y have different sizes: ${x.length} != ${y.length}")
    assert(decay > 0 && decay <= 1, s"decay factor must be in range (0, 1]: $decay")
    import scala.sys.process._
    val cmd = "./plotting/curve_fitter.py %s %s %s %s %s"
      .format(func.name, x.mkString(","), y.mkString(","), decay, startingParameters.mkString(","))
    val output: String = cmd.trim().!!
    fittedParams = output.trim().split(", ").map(_.toDouble)
  }

}
