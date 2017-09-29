package benchmarks.wordcount

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object WordCount {

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("WordCount")
		val ssc = new StreamingContext(conf, Seconds(1))
        val topics = Array("HTK")
        val kafkaParams = Map(
            "bootstrap.servers" -> (1 to 30).map(i => s"worker$i:9093").mkString(","),
  			"key.deserializer" -> classOf[StringDeserializer],
  			"value.deserializer" -> classOf[StringDeserializer],
  			"auto.offset.reset" -> "earliest",
  			"client.id" -> "test-client",
		    "group.id" -> "spark_wordcount"
        )

        val text = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        text.countByWindow(Seconds(1), Seconds(1)).print()

        ssc.checkpoint("hdfs://master:9000/liuzhi/spark-checkpoint")
		ssc.start()
		ssc.awaitTermination()	
	}
}
