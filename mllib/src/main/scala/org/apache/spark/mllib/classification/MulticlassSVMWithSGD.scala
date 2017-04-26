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

package org.apache.spark.mllib.classification
// scalastyle:off
import breeze.linalg.{norm, DenseVector => BDV}
import org.apache.spark.PoolReweighterLoss

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.{Gradient, SquaredL2Updater, Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
// scalastyle: on


class MulticlassSVMModel(classModels: Array[SVMModel])
  extends ClassificationModel with Serializable {

  val classModelsWithIndex = classModels.zipWithIndex

  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val localClassModelsWithIndex = classModelsWithIndex
    val bcClassModels = testData.context.broadcast(localClassModelsWithIndex)
    testData.mapPartitions { iter =>
      val w = bcClassModels.value
      iter.map(v => predictPoint(v, w))
    }
  }

  override def predict(testData: Vector): Double = predictPoint(testData, classModelsWithIndex)

  def predictPoint(testData: Vector, models: Array[(SVMModel, Int)]): Double =
    models
      .map { case (classModel, classNumber) => (classModel.predict(testData), classNumber)}
      .maxBy { case (score, classNumber) => score}
      ._2

}


object MulticlassSVMWithSGD {

  def train(
             input: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double,
             numClasses: Int): MulticlassSVMModel = {

    // val numClasses = 10
    val numFeatures = input.map(_.features.size).first()
    // val numOfLinearPredictor = numClasses - 1
    val initialWeights = (1 to numClasses).map {
      x => Vectors.zeros(numFeatures)
    }

    val data = input.map(lp => (lp.label, lp.features))

    val updater = new SquaredL2Updater()
    val convergenceTol = 0.00001

    val (weights, lossHistory) = GradientDescent.runMiniBatchSGD(
      data,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFraction,
      initialWeights,
      convergenceTol,
      numClasses)

    val classModels = weights.map { w =>
      val model = new SVMModel(w, 0.0)
      model.clearThreshold()
      model
    }
    new MulticlassSVMModel(classModels)

  }

  def train(input: RDD[LabeledPoint], numIterations: Int,
            stepSize: Double, regParam: Double): MulticlassSVMModel =
  train(input, numIterations, stepSize, regParam, 1.0, 2)

  def train(input: RDD[LabeledPoint], numIterations: Int): MulticlassSVMModel =
  train(input, numIterations, 1.0, 0.01, 1.0, 2)

}

object GradientDescent extends Logging {
  /**
    * Run stochastic gradient descent (SGD) in parallel using mini batches.
    * In each iteration, we sample a subset (fraction miniBatchFraction) of the total data
    * in order to compute a gradient estimate.
    * Sampling, and averaging the subgradients over this subset is performed using one standard
    * spark map-reduce in each iteration.
    *
    * @param data Input data for SGD. RDD of the set of data examples, each of
    *             the form (label, [feature values]).
    * @param updater Updater function to actually perform a gradient step in a given direction.
    * @param stepSize initial step size for the first step
    * @param numIterations number of iterations that SGD should be run.
    * @param regParam regularization parameter
    * @param miniBatchFraction fraction of the input data set that should be used for
    *                          one iteration of SGD. Default value 1.0.
    * @param convergenceTol Minibatch iteration will end before numIterations if the relative
    *                       difference between the current weight and the previous weight is less
    *                       than this value. In measuring convergence, L2 norm is calculated.
    *                       Default value 0.001. Must be between 0.0 and 1.0 inclusively.
    * @return A tuple containing two elements. The first element is a column matrix containing
    *         weights for every feature, and the second element is an array containing the
    *         stochastic loss computed for every iteration.
    */
  def runMiniBatchSGD(
   data: RDD[(Double, Vector)],
   updater: Updater,
   stepSize: Double,
   numIterations: Int,
   regParam: Double,
   miniBatchFraction: Double,
   initialWeights: Seq[Vector],
   convergenceTol: Double,
   numClasses: Int): (Array[Vector], Array[ArrayBuffer[Double]]) = {

    // convergenceTol should be set with non minibatch settings
    if (miniBatchFraction < 1.0 && convergenceTol > 0.0) {
      logWarning("Testing against a convergenceTol when using miniBatchFraction " +
        "< 1.0 can be unstable because of the stochasticity in sampling.")
    }

    if (numIterations * miniBatchFraction < 1.0) {
      logWarning("Not all examples will be used if numIterations * miniBatchFraction < 1.0: " +
        s"numIterations=$numIterations and miniBatchFraction=$miniBatchFraction")
    }

    val stochasticLossHistory = (1 to numClasses).map { _ => new ArrayBuffer[Double](numIterations) }.toArray
    // Record previous weight and current one to calculate solution vector difference

    val previousWeights: Array[Option[Vector]] = (1 to numClasses).map { _ => None }.toArray
    val currentWeights: Array[Option[Vector]] = (1 to numClasses).map { _ => None }.toArray
    val gradient = (0 to numClasses - 1).map { i =>
      new MultiClassHingeGradient(i)
    }

    val numExamples = data.count()


    if (numExamples * miniBatchFraction < 1) {
      logWarning("The miniBatchFraction is too small")
    }

    // Initialize weights as a column vector
    val weights = initialWeights.map {
      x => Vectors.dense(x.toArray)
    }.toArray
    val n = weights(0).size

//    PoolReweighterLoss.updateModel(new MulticlassSVMModel(classModels))

    /**
      * For the first iteration, the regVal will be initialized as sum of weight squares
      * if it's L2 updater; for L1 updater, the same logic is followed.
      */
    val regVal = weights.map {
      w => updater.compute(
        w, Vectors.zeros(w.size), 0, 1, regParam)._2
    }

    var converged = false // indicates whether converged based on convergenceTol
    var i = 1
    while (!converged && i <= numIterations) {
      var sum = 0.0
      (0 to numClasses - 1).map {
        k => {
          val bcWeights = data.context.broadcast(weights(k))
          val (gradientSum, lossSum, miniBatchSize) = data.sample(false, miniBatchFraction, 42 + i)
            .treeAggregate((BDV.zeros[Double](n), 0.0, 0L))(
              seqOp = (c, v) => {
                // c: (grad, loss, count), v: (label, features)
                val l = gradient(k).compute(v._2, v._1, bcWeights.value, Vectors.fromBreeze(c._1))
                (c._1, c._2 + l, c._3 + 1)
              },
              combOp = (c1, c2) => {
                // c: (grad, loss, count)
                (c1._1 += c2._1, c1._2 + c2._2, c1._3 + c2._3)
              })
          stochasticLossHistory(k) += lossSum / miniBatchSize + regVal(k)
          sum += lossSum / miniBatchSize + regVal(k)
          val update = updater.compute(
            weights(k), Vectors.fromBreeze(gradientSum / miniBatchSize.toDouble),
            stepSize, i, regParam)
          weights(k) = update._1
          regVal(k) = update._2

          previousWeights(k) = currentWeights(k)
          currentWeights(k) = Some(weights(k))
//          if(currentWeights(k) != None) {
//            // create temp model and validate
//            val model = new SVMModel(currentWeights(k).get, 0)
//            model.clearThreshold()
//            models(k) = model
//          }
          // scalastyle:
//          if (previousWeights(k) != None && currentWeights(k) != None) {
//            converged = converged && isConverged(previousWeights(k).get,
//              currentWeights(k).get, convergenceTol)
//          }
        }
      }
      PoolReweighterLoss.updateLoss(sum / numClasses)
      i += 1
    }
    (weights, stochasticLossHistory)
  }


  private def isConverged(
     previousWeights: Vector,
     currentWeights: Vector,
     convergenceTol: Double): Boolean = {
    // To compare with convergence tolerance.
    val previousBDV = previousWeights.asBreeze.toDenseVector
    val currentBDV = currentWeights.asBreeze.toDenseVector

    // This represents the difference of updated weights in the iteration.
    val solutionVecDiff: Double = norm(previousBDV - currentBDV)

    solutionVecDiff < convergenceTol * Math.max(norm(currentBDV), 1.0)
  }

}

class MultiClassHingeGradient(cn: Int) extends Gradient {
  val classNum = cn


  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    val dotProduct = dot(data, weights)
    // Our loss function with {0, 1} labels is max(0, 1 - (2y - 1) (f_w(x)))
    // Therefore the gradient is -(2y - 1)*x
    val labelScaled = if (label == classNum) 1.0 else -1.0
    if (1.0 > labelScaled * dotProduct) {
      val gradient = data.copy
      scal(-labelScaled, gradient)
      (gradient, 1.0 - labelScaled * dotProduct)
    } else {
      (Vectors.sparse(weights.size, Array.empty, Array.empty), 0.0)
    }
  }

  override def compute(
    data: Vector,
    label: Double,
    weights: Vector,
    cumGradient: Vector): Double = {
    val dotProduct = dot(data, weights)
    // Our loss function with {0, 1} labels is max(0, 1 - (2y - 1) (f_w(x)))
    // Therefore the gradient is -(2y - 1)*x
    val labelScaled = if (label == classNum) 1.0 else -1.0
    if (1.0 > labelScaled * dotProduct) {
      axpy(-labelScaled, data, cumGradient)
      1.0 - labelScaled * dotProduct
    } else {
      0.0
    }
  }
}

