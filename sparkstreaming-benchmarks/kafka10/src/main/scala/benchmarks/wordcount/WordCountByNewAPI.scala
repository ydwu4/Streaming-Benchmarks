package benchmarks.wordcount

import joptsimple.OptionParser
import kafka.utils.CommandLineUtils
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.util.Random

object WordCountByNewAPI {

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    val config = new WordCountConfig(args)
    val conf = new SparkConf().setAppName("WordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val topics = Array(config.topic)

    println(s"topic: ${config.topic}.")
    println(s"auto.offset.reset: ${config.autoOffsetReset}.")
    println(s"group.id: ${config.groupId}.")
    println(s"enable.auto.commit: ${config.enableAutoCommit}.")

    val kafkaParams = Map(
      "bootstrap.servers" -> (1 to 30).map(i => s"worker$i:9093").mkString(","),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> config.autoOffsetReset,
      "group.id" -> config.groupId,
      "enable.auto.commit" -> config.enableAutoCommit
    )

    val text = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val lines = text.map(_.value)
    //    val words1 = lines.flatMap(_.split(" "))
    //    val wordCounts = words1.map(x => (x, 1L)).reduceByKey(_ + _)
    //    val wordCounts = words.map({tuple =>
    //        val timeStamp = tuple._1
    //        val word = tuple._2
    //        (word, new Tuple2[String, Long](timeStamp, 1L))
    //    }).reduceByKey({ (accm, tuple) =>
    //        val curTimeStamp = System.currentTimeMillis()
    //        val timeStamp = tuple._1.toLong
    //        Logger.getRootLogger.info(s"Latency: ${curTimeStamp-timeStamp}")
    //        log.info(s"Latency: ${curTimeStamp-timeStamp}")
    //        (accm._1, accm._2+tuple._2)
    //    })

    val words = lines.flatMap({line =>
        val wds = line.split(" ")
        val tp = wds(0)
        val wd_tp = wds.slice(1,wds.length).map({wd =>
          (wd, (tp,1))
        })
        wd_tp
    })
    // Update the cumulative count using mapWithState
    // This will give a DStream made of state (which is the cumulative count of the words)
    val mappingFunc = (word: String, one: Option[(String,Int)], state: State[Int]) => {
      val (tp, count) = one.getOrElse(("0",0))
      if (!tp.equals("0")) {
        val cur_tp = System.currentTimeMillis()
        log.warn(s"Latency: ${cur_tp - tp.toLong} milliseconds")
      }
      val sum = count + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }
    val initialRDD = ssc.sparkContext.parallelize(List[(String,Int)]());
    StateSpec.function(mappingFunc).initialState(initialRDD)
    val wordCounts = words.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

  class WordCountConfig(args: Array[String]) {
    val rand = new Random()
    val parser = new OptionParser(false)
    val topicIdOpt = parser.accepts("topic", "[REQUIRED] topic")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val autoOffsetResetOpt = parser.accepts("auto.offset.reset", "auto.offset.reset")
      .withRequiredArg()
      .defaultsTo("earliest")
      .describedAs("auto.offset.reset")
      .ofType(classOf[String])
    val groupIdOpt = parser.accepts("group.id", "group.id")
      .withRequiredArg()
      .defaultsTo(s"spark-wordcount-${rand.nextLong()}")
      .describedAs("group.id")
      .ofType(classOf[String])
    val enableAutoCommitOpt = parser.accepts("enable.auto.commit", "enable.auto.commit")
      .withRequiredArg()
      .defaultsTo("false")
      .describedAs("enable.auto.commit")
      .ofType(classOf[String])

    val options = parser.parse(args: _*)
    CommandLineUtils.checkRequiredArgs(parser, options, topicIdOpt)
    val topic = options.valueOf(topicIdOpt)
    val autoOffsetReset = options.valueOf(autoOffsetResetOpt)
    val groupId = options.valueOf(groupIdOpt)
    val enableAutoCommit = options.valueOf(enableAutoCommitOpt)
  }
}
