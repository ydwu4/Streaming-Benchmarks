package benchmarks.wordcount

import java.util

import joptsimple.OptionParser
import kafka.utils.CommandLineUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.Random

object WordCountByOldAPI {
  def main(args: Array[String]) {
    val config = new WordCountConfig(args)
    val Array(zkQuorum, group, topic, numThreads) = Array(config.zkQuorum, config.groupId, config.topic, config.numThreads)
    println(s"zkQuorum: $zkQuorum")
    println(s"group.id: $group")
    println(s"topic: $topic")
    println(s"nmThreads: $numThreads")
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topic.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

  class WordCountConfig(args: Array[String]) {
    val rand = new Random()
    val parser = new OptionParser(false)
    val topicOpt = parser.accepts("topic", "[REQUIRED] topic")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val zkQuorumOpt = parser.accepts("zkQuorum", "[REQUIRED] zkQuorum")
      .withRequiredArg()
      .describedAs("zkQuorum")
      .ofType(classOf[String])
    val groupIdOpt = parser.accepts("group.id", "group.id")
      .withRequiredArg()
      .defaultsTo(s"spark-wordcountbyoldapi-${rand.nextLong()}")
      .describedAs("group.id")
      .ofType(classOf[String])
    val numThreadsOpt = parser.accepts("numThreads", "[REQUIRED] numThreads")
      .withRequiredArg()
      .describedAs("numThreads")
      .ofType(classOf[String])

    val options = parser.parse(args: _*)
    CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numThreadsOpt, zkQuorumOpt)
    val topic = options.valueOf(topicOpt)
    val zkQuorum = options.valueOf(zkQuorumOpt)
    val groupId = options.valueOf(groupIdOpt)
    val numThreads = options.valueOf(numThreadsOpt)
  }

}
