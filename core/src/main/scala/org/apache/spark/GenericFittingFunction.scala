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


/**
 * Function that represents 1 / (ax + b).
 */
class OneOverXFunction extends GenericFittingFunction("one_over_x", 2) {
  protected override def doCompute(x: Double, params: Seq[Double]): Double = {
    val (a, b) = (params(0), params(1))
    1 / (a * x + b)
  }
}


/**
 * Function that represents 1 / (ax**2 + bx + c)
 */
class OneOverXSquaredFunction extends GenericFittingFunction("one_over_x_squared", 3) {
  protected override def doCompute(x: Double, params: Seq[Double]): Double = {
    val (a, b, c) = (params(0), params(1), params(2))
    1 / (a * math.pow(x, 2) +  b * x + c)
  }
}


/**
 * Abstract fitting function that validates number of parameters before computing
 * the value or the gradient of the function.
 */
abstract class GenericFittingFunction(val name: String, val numParameters: Int) {

  private def validateNumParams(params: Seq[Double]): Unit = {
    assert(params.length == numParameters,
      s"expected $numParameters parameters, got ${params.length}: ${params.mkString(", ")}")
  }

  def compute(x: Double, params: Double*): Double = {
    validateNumParams(params)
    doCompute(x, params)
  }

  protected def doCompute(x: Double, params: Seq[Double]): Double
}
