package benchmarks.advertising

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.json.JSONObject
import org.sedis.Dress
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.Iterator
import scala.collection.mutable.{Map => MMap}
import scala.compat.Platform.currentTime

// Follow https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
object AdvertisingSql {
  def parse(args: Array[String]): Map[String, String] = {
    var config = Map.empty[String, String]
    for (i <- 0 until(args.length, 2)) {
      config += args(i) -> args(i + 1)
    }
    config
  }

  def main(args: Array[String]): Unit = {
    val commonConfig = parse(args)
    val batchTime = commonConfig.getOrElse("spark.batchtime", "0").toLong
    val windowTime = commonConfig.getOrElse("window.time", "10").toLong
    val windowDelay = commonConfig.getOrElse("window.delay", "10").toLong
    val topic = commonConfig.getOrElse("kafka.topic", "SparkAdvertisingStream")
    val redisHost = commonConfig("redis.host")
    val redisPort = commonConfig.getOrElse("redis.port", "6379").toInt
    val kafkaPort = commonConfig("kafka.port").toInt
    val kafkaHosts = commonConfig("kafka.brokers").split(",").toSeq

    // Create direct kafka stream with brokers and topics
    val brokers = kafkaHosts.map(broker => s"$broker:$kafkaPort").mkString(",")
    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> brokers,
      "auto.offset.reset" -> "smallest",
      "subscribe" -> topic
    )

    val spark = SparkSession.builder().appName("AdvertisingStream").config(new SparkConf()).getOrCreate()
    import spark.implicits._
    val messages = spark.readStream.format("kafka").options(kafkaParams).load()

    //We can repartition to use more executors if desired
    //    val messages_repartitioned = messages.repartition(10)


    //take the second tuple of the implicit Tuple2 argument _, by calling the Tuple2 method ._2
    //The first tuple is the key, which we don't use in this benchmark
    val kafkaRawData = messages.selectExpr("CAST(value AS STRING)").as[String] // ConsumedRecord -> get value, json message

    //Parse the String as JSON
    val kafkaData = kafkaRawData.map(parseJson) // (user_id, page_id, ad_id, ad_type, event_type, event_time, ip_address)

    //Filter the records if event type is "view"
    val filteredOnView = kafkaData.filter(_._5 == "view") // filter event_type == "view", ad is viewed

    //project the event, basically filter the fileds.
    val projected = filteredOnView.map(eventProjection) // (ad_id, event_time)

    //Note that the Storm benchmark caches the results from Redis, we don't do that here yet
    val redisJoined = projected.mapPartitions(i => queryRedisTopLevel(i, redisHost, redisPort)) // (campaign_id, ad_id, event_time)

    val campaignIdWithTime = redisJoined.map {
      case (campaignID, adID, eventTime) => (campaignID, new Timestamp(eventTime.toLong))
    } // (campaign_id, event_time)

    val windowedCount = // count for each campaign per window
      campaignIdWithTime.toDF("cid", "time")
        .withWatermark("time", s"$windowDelay second")
        .groupBy(window($"time", s"$windowTime second", s"$windowTime second"), $"cid")
        .count()

    val writeToRedis = windowedCount.writeStream
      .trigger(Trigger.ProcessingTime(batchTime))
      .foreach(new ForeachWriter[Row] {
        private var pool: Option[JedisPool] = None
        private var jedis: Option[Jedis] = None
        private var wrap: Option[Dress.Wrap] = None

        override def process(value: Row): Unit = {
          val timestamp = value.getAs[GenericRowWithSchema](0).getTimestamp(0).getTime.toString
          val campaign = value.getString(1)
          val window_seenCount = value.getLong(2)

          val redis = wrap.get

          var windowUUID = redis.hmget(campaign, timestamp).head
          if (windowUUID == null) {
            windowUUID = UUID.randomUUID().toString
            redis.hset(campaign, timestamp, windowUUID)
            var windowListUUID = redis.hmget(campaign, "windows").head
            if (windowListUUID == null) {
              windowListUUID = UUID.randomUUID().toString
              redis.hset(campaign, "windows", windowListUUID)
            }
            redis.lpush(windowListUUID, timestamp)
          }
          redis.hincrBy(windowUUID, "seen_count", window_seenCount)
          redis.hset(windowUUID, "time_updated", currentTime.toString)
        }

        override def close(errorOrNull: Throwable): Unit = {
          pool.foreach(_.destroy())
          jedis.foreach(_.close())
          pool = None
          jedis = None
          wrap = None
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          try {
            pool = Some(new JedisPool(new JedisPoolConfig(), redisHost, redisPort, 2000))
            jedis = Some(pool.get.getResource)
            wrap = Some(Dress.up(jedis.get))
            true
          } catch {
            case _: Throwable => false
          }
        }
      }).start()

    writeToRedis.awaitTermination()
    spark.close()
  }

  def parseJson(jsonString: String): (String, String, String, String, String, String, String) = {
    val parser = new JSONObject(jsonString)
    (parser.getString("user_id"),
      parser.getString("page_id"),
      parser.getString("ad_id"),
      parser.getString("ad_type"),
      parser.getString("event_type"),
      parser.getString("event_time"),
      parser.getString("ip_address"))
  }

  def eventProjection(event: (String, String, String, String, String, String, String)): (String, String) = {
    (event._3, event._6) //ad_id, event_time
  }

  def queryRedisTopLevel(eventsIterator: Iterator[(String, String)], redisHost: String, redisPort: Int): Iterator[(String, String, String)] = {
    val pool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort, 2000)
    val resource = pool.getResource
    val wrap = Dress.up(resource)
    val ad_to_campaign = MMap.empty[String, String]
    val eventsIteratorMap = eventsIterator.map(event => queryRedis(wrap, ad_to_campaign, event))
    resource.close()
    pool.destroy()
    eventsIteratorMap
  }

  /**
    * Q: Why we need to keep ad_id here
    */
  def queryRedis(redis: Dress.Wrap, ad_to_campaign: MMap[String, String], event: (String, String)): (String, String, String) = {
    val ad_id = event._1
    val campaign_id = ad_to_campaign.getOrElse(ad_id, {
      redis.get(ad_id) match {
        case Some(cid) =>
          ad_to_campaign += ad_id -> cid; cid
        case None =>
          "Campaign_ID not found in either cache nore Redis for the given ad_id!"
      }
    })
    (campaign_id, event._1, event._2)
  }
}
