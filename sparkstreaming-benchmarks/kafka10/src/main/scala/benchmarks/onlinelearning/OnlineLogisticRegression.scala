package benchmarks.onlinelearning

import java.util.logging.Logger

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.annotation.tailrec

object OnlineLogisticRegression {
  private lazy val LOG = Logger.getLogger(OnlineLogisticRegression.getClass.getName)

  @tailrec
  def parse(args: List[String], config: Map[String, String]): Map[String, String] = {
    args match {
      case Nil => config
      case key :: value :: tail => parse(tail, config + (key -> value))
      case key :: Nil => throw new Exception("Ahhh")
    }
  }

  def labelize(label: Int, minL: Int, maxL: Int): Int = {
    (maxL - label) / (maxL - minL) // to 0 or 1
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("online logreg alpha")
    val config = parse(args.toList, Map.empty)
    val batchTime = config.getOrElse("batch.time", "1").toInt
    val ssc = new StreamingContext(conf, Seconds(batchTime))
    val numFeatures = config("feature.num").toInt
    val minL = config("label.min").toInt
    val maxL = config("label.max").toInt
    val iter = config.getOrElse("iteration.num", "1").toInt

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> config.getOrElse("group.id", "spark-logreg"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(config.getOrElse("topic", "spark-logreg"))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val trainingData = stream.map(_.value()).map {
      line =>
        val sp = line.split("\\s")
        val time = sp.head.toLong
        val tail = sp.tail.tail.map(_.split(":")).map{ iv => (iv(0).toInt, iv(1).toDouble) }
        val label = labelize(sp.tail.head.toInt, minL, maxL)
        (time, new LabeledPoint(label, Vectors.sparse(numFeatures, tail.map(_._1), tail.map(_._2))))
    }
    
    val model = new StreamingLogisticRegressionWithSGD()
        .setInitialWeights(Vectors.zeros(numFeatures))
        .setNumIterations(iter)

    model.trainOn(trainingData.map(_._2))

    trainingData.map(_._1).foreachRDD {
      (rdd, time) =>
        val timeMillis = time.milliseconds
        val numElem = rdd.count()
        val avgLatency = rdd.map(eventTime => timeMillis - eventTime).sum / numElem.toDouble
        println(s"Number of elements: $numElem, Latency: $avgLatency, batchTimeStamp: $timeMillis(${new java.util.Date(timeMillis)})")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
