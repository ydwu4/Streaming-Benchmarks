package benchmarks.onlinelearning

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.StreamingLinearAlgorithm

class SVMWithSGDX extends SVMWithSGD {
  /**
    * To make protected method `createModel` public
    */
  def model(weights: Vector, intercept: Double): SVMModel = createModel(weights, intercept)
}

/**
  * Modify from
  * https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/regression/StreamingLinearRegressionWithSGD.scala
  */
class StreamingSVMWithSGD
(
  private var stepSize: Double,
  private var numIterations: Int,
  private var regParam: Double,
  private var miniBatchFraction: Double
) extends StreamingLinearAlgorithm[SVMModel, SVMWithSGDX] with Serializable {

  def this() = this(0.1, 50, 0.0, 1.0)

  protected val algorithm: SVMWithSGDX = {
    val algo = new SVMWithSGDX()
    algo.optimizer.setStepSize(stepSize)
    algo.optimizer.setNumIterations(numIterations)
    algo.optimizer.setRegParam(regParam)
    algo.optimizer.setMiniBatchFraction(miniBatchFraction)
    algo
  }

  protected var model: Option[SVMModel] = None

  /** Set the number of iterations of gradient descent to run per update. Default: 50. */
  def setNumIterations(numIterations: Int): this.type = {
    this.algorithm.optimizer.setNumIterations(numIterations)
    this
  }

  def setStepSize(stepSize: Int): this.type = {
    this.algorithm.optimizer.setStepSize(stepSize)
    this
  }

  def setMiniBatchFraction(miniBatchFraction: Double): this.type = {
    this.algorithm.optimizer.setMiniBatchFraction(miniBatchFraction)
    this
  }

  /** Set the regularization parameter. Default: 0.0. */
  def setRegParam(regParam: Double): this.type = {
    this.algorithm.optimizer.setRegParam(regParam)
    this
  }

  /** Set the initial weights. Default: [0.0, 0.0]. */
  def setInitialWeights(initialWeights: Vector): this.type = {
    this.model = Some(algorithm.model(initialWeights, 0.0))
    this
  }
}
