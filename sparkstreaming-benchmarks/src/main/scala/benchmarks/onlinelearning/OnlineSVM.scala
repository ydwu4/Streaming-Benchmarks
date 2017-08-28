package benchmarks.onlinelearning

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.annotation.tailrec

object OnlineSVM {
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
    val conf = new SparkConf().setAppName("online svm alpha")
    val ssc = new StreamingContext(conf, Seconds(1))
    val config = parse(args.toList, Map.empty)
    val numFeatures = config("feature.num").toInt
    val minL = config("label.min").toInt
    val maxL = config("label.max").toInt

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config("bootstrap.servers"),
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> config.getOrElse("group.id", "spark-svm"),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(config.getOrElse("topic", "spark-svm"))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val trainingData = stream.map(_.value()).map {
      line =>
        val sp = line.split("\\s")
        val tail = sp.tail.map(_.split(":")).map { iv => (iv(0).toInt, iv(1).toDouble) }
        val label = labelize(sp.head.toInt, minL, maxL)
        new LabeledPoint(label, Vectors.sparse(numFeatures, tail.map(_._1), tail.map(_._2)))
    }

    val model = new StreamingSVMWithSGD().setInitialWeights(Vectors.zeros(numFeatures))

    model.trainOn(trainingData)

    ssc.start()
    ssc.awaitTermination()
  }
}
