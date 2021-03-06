package benchmarks.advertising

import java.util
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.json.JSONObject
import org.sedis.{Dress, Pool}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.Iterator
import scala.compat.Platform.currentTime

object Advertising {
  def parse(args: Array[String]): Map[String, String] = {
    var config = Map.empty[String, String]
    for (i <- 0 until(args.length, 2)) {
      config += args(i) -> args(i + 1)
    }
    config
  }

  def main(args: Array[String]) {
    val commonConfig = parse(args)
    val batchSize = commonConfig.getOrElse("spark.batchtime", "5000").toInt
    val topic = commonConfig.getOrElse("kafka.topic", "SparkAdvertisingStream")
    val redisHost = commonConfig("redis.host")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("AdvertisingStream")
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))

    val kafkaPort = commonConfig("kafka.port").toInt
    val kafkaHosts = commonConfig("kafka.brokers").split(",").toSeq

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val brokers = kafkaHosts.map(broker => s"$broker:$kafkaPort").mkString(",")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    System.err.println("Trying to connect to Kafka at " + brokers)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topicsSet, kafkaParams))

    //We can repartition to use more executors if desired
    //    val messages_repartitioned = messages.repartition(10)


    //take the second tuple of the implicit Tuple2 argument _, by calling the Tuple2 method ._2
    //The first tuple is the key, which we don't use in this benchmark
    val kafkaRawData = messages.map(_.value())

    //Parse the String as JSON
    val kafkaData = kafkaRawData.map(parseJson)

    //Filter the records if event type is "view"
    val filteredOnView = kafkaData.filter(_ (4) == "view")

    //project the event, basically filter the fileds.
    val projected = filteredOnView.map(eventProjection)

    //Note that the Storm benchmark caches the results from Redis, we don't do that here yet
    val redisJoined = projected.mapPartitions(i => queryRedisTopLevel(i, redisHost), preservePartitioning = false)

    val campaign_timeStamp = redisJoined.map(campaignTime)
    //each record in the RDD: key:(campaign_id : String, window_time: Long),  Value: (ad_id : String)
    //DStream[((String,Long),String)]

    // since we're just counting use reduceByKey
    val totalEventsPerCampaignTime = campaign_timeStamp.mapValues(_ => 1).reduceByKey(_ + _)

    //DStream[((String,Long), Int)]
    //each record: key:(campaign_id, window_time),  Value: number of events

    //Repartition here if desired to use more or less executors
    //    val totalEventsPerCampaignTime_repartitioned = totalEventsPerCampaignTime.repartition(20)

    totalEventsPerCampaignTime.foreachRDD { rdd =>
      rdd.foreachPartition(writeRedisTopLevel(_, redisHost))
    }

    // Start the computation
    ssc.start
    ssc.awaitTermination
  }

  def parseJson(jsonString: String): Array[String] = {
    val parser = new JSONObject(jsonString)
    Array(
      parser.getString("user_id"),
      parser.getString("page_id"),
      parser.getString("ad_id"),
      parser.getString("ad_type"),
      parser.getString("event_type"),
      parser.getString("event_time"),
      parser.getString("ip_address"))
  }

  def eventProjection(event: Array[String]): Array[String] = {
    Array(
      event(2), //ad_id
      event(5)) //event_time
  }

  def queryRedisTopLevel(eventsIterator: Iterator[Array[String]], redisHost: String): Iterator[Array[String]] = {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))
    val ad_to_campaign = new util.HashMap[String, String]()
    val eventsIteratorMap = eventsIterator.map(event => queryRedis(pool, ad_to_campaign, event))
    pool.underlying.getResource.close()
    eventsIteratorMap
  }

  def queryRedis(pool: Pool, ad_to_campaign: util.HashMap[String, String], event: Array[String]): Array[String] = {
    val ad_id = event(0)
    val campaign_id_cache = ad_to_campaign.get(ad_id)
    if (campaign_id_cache == null) {
      pool.withJedisClient { client =>
        val campaign_id_temp = Dress.up(client).get(ad_id)
        if (campaign_id_temp.isDefined) {
          val campaign_id = campaign_id_temp.get
          ad_to_campaign.put(ad_id, campaign_id)
          Array(campaign_id, event(0), event(1))
          //campaign_id, ad_id, event_time
        } else {
          Array("Campaign_ID not found in either cache nore Redis for the given ad_id!", event(0), event(1))
        }
      }
    } else {
      Array(campaign_id_cache, event(0), event(1))
    }
  }

  /**
    * 1. Event time
    * 2. Time window length: time_divisor 10000L
    * 3. Non-overlapping
    */
  def campaignTime(event: Array[String]): ((String, Long), String) = {
    val time_divisor: Long = 10000L
    ((event(0), time_divisor * (event(2).toLong / time_divisor)), event(1))
    //Key: (campaign_id, window_time),  Value: ad_id
  }

  def writeRedisTopLevel(campaign_window_counts_Iterator: Iterator[((String, Long), Int)], redisHost: String) {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))
    campaign_window_counts_Iterator.foreach(campaign_window_counts => writeWindow(pool, campaign_window_counts))
    pool.underlying.getResource.close()
  }

  private def writeWindow(pool: Pool, campaign_window_counts: ((String, Long), Int)): String = {
    val ((campaign, window_timestamp), window_seenCount) = campaign_window_counts
    val timestamp = window_timestamp.toString
    pool.withJedisClient {
      client =>
        val dressUp = Dress.up(client)
        var windowUUID = dressUp.hmget(campaign, timestamp).head
        if (windowUUID == null) {
          windowUUID = UUID.randomUUID().toString
          dressUp.hset(campaign, timestamp, windowUUID)
          var windowListUUID: String = dressUp.hmget(campaign, "windows").head
          if (windowListUUID == null) {
            windowListUUID = UUID.randomUUID.toString
            dressUp.hset(campaign, "windows", windowListUUID)
          }
          dressUp.lpush(windowListUUID, timestamp)
        }
        dressUp.hincrBy(windowUUID, "seen_count", window_seenCount)
        dressUp.hset(windowUUID, "time_updated", currentTime.toString)
        window_seenCount.toString
    }
  }
}