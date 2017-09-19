package benchmarks.advertising;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class AdvertisingByDefaultWindow {
    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingByDefaultWindow.class);

    // modified from https://github.com/yahoo/streaming-benchmarks/blob/master/flink-benchmarks/src/main/java/flink/benchmark/AdvertisingTopologyNative.java
    static public void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        int kafkaPartitions = params.getInt("kafka.partitions");
        int hosts = params.getInt("process.hosts");
        int cores = params.getInt("process.cores");
        // Since we are using event time,
        // there will be watermarks flowing in the topologies.
        // Since the time is windowed by event time, a window closes itself only after
        // the event time represented by watermarks passes end of the window.
        // However even the watermarks may be behind the real event time,
        // there will still be some really late records flowing in,
        // So allowed lateness will allow these really late records to be added in the window.
        int allowedLateness = params.getInt("allowed.lateness", 10);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(params.getLong("flink.buffer-timeout", 100));

        if (params.has("flink.checkpoint-interval")) {
            // enable checkpointing for fault tolerance
            env.enableCheckpointing(params.getLong("flink.checkpoint-interval", 1000));
        }
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
        env.setParallelism(hosts * cores);

        // set the time characteristic to be event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer010<String>(
                        params.getRequired("topic"),
                        new SimpleStringSchema(),
                        params.getProperties())).setParallelism(Math.min(hosts * cores, kafkaPartitions));

        messageStream
                .rebalance()
                // Parse the String as JSON
                .flatMap(new Advertising.DeserializeBolt())
                //Filter the records if event type is "view"
                .filter(new Advertising.EventFilterBolt())
                // project the event
                .<Tuple2<String, String>>project(2, 5)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, String>>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, String> record) {
                        Long event_time = Long.parseLong(record.f1);
                        return event_time;
                    }
                })
                // perform join with redis data
                .flatMap(new Advertising.RedisJoinBolt())
                // process campaign
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CampaignProcess());

        env.execute();
    }


    public static class CampaignProcess extends RichWindowFunction<Tuple2<String, String>, String, Tuple, TimeWindow> {

        private static final Logger LOG = LoggerFactory.getLogger(CommonCampaignProcessor.class);
        private Jedis flush_jedis;
        private Long lastWindowMillis;
        private String redisServerHostname;
        // Bucket -> Campaign_id -> Window
        private LRUHashMap<Long, HashMap<String, Window>> campaign_windows;
        private Set<CampaignWindowPair> need_flush;
        
        private static final Long time_divisor = 10000L; // 10 second windows

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, String>> input, Collector<String> out) throws Exception {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            redisServerHostname = parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            flush_jedis = new Jedis(redisServerHostname);
            LOG.info("Opened connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            campaign_windows = new LRUHashMap<>(10);
            need_flush = new HashSet<CampaignWindowPair>();

            // iterate over the incoming tuples
            // and buffer all the changes should be made to redis in need_flush
            for (Tuple2<String, String> record : input) {
                String campaignId = record.getField(0);
                String eventTime = record.getField(1);
                Long timeBucket = Long.parseLong(eventTime) / time_divisor;
                Window myWindow = getWindow(timeBucket, campaignId);
                myWindow.seenCount++;
                CampaignWindowPair newPair = new CampaignWindowPair(campaignId, myWindow);
                need_flush.add(newPair);
            }

            flushWindows();
        }

        public void flushWindows() {
            for (CampaignWindowPair pair : need_flush) {
                writeWindow(pair.campaign, pair.window);
            }
            need_flush.clear();
        }

        // called by flushWindows, so basically this function will be invoked periodically to update the redis
        private void writeWindow(String campaign, Window win) {
            // there is a unique window ID with every window of every campaign
            String windowUUID = flush_jedis.hmget(campaign, win.timestamp).get(0);
            if (windowUUID == null) {
                windowUUID = UUID.randomUUID().toString();
                flush_jedis.hset(campaign, win.timestamp, windowUUID);

                // there is a unique window list ID with every campaign
                String windowListUUID = flush_jedis.hmget(campaign, "windows").get(0);
                if (windowListUUID == null) {
                    windowListUUID = UUID.randomUUID().toString();
                    flush_jedis.hset(campaign, "windows", windowListUUID);
                }
                flush_jedis.lpush(windowListUUID, win.timestamp);
            }

            synchronized (campaign_windows) {
                flush_jedis.hincrBy(windowUUID, "seen_count", win.seenCount);
                win.seenCount = 0L;
            }
            flush_jedis.hset(windowUUID, "time_updated", Long.toString(System.currentTimeMillis()));
            flush_jedis.lpush("time_updated", Long.toString(System.currentTimeMillis()));
        }

        // since timeBucket is event / time_divisor (time_divisor is the duration of the window
        // so timeBucket * time_divisor would be the starting time of a window
        public Window redisGetWindow(Long timeBucket, Long time_divisor) {
            Window win = new Window();
            win.timestamp = Long.toString(timeBucket * time_divisor);
            win.seenCount = 0L;
            return win;
        }

        public Window getWindow(Long timeBucket, String campaign_id) {
            synchronized (campaign_windows) {
                HashMap<String, Window> bucket_map = campaign_windows.get(timeBucket);
                if (bucket_map == null) {
                    // we construct a window by ourselves
                    // windows's timestamp is time_divisor's multiples
                    // in other words, it's the starting time of a window
                    Window redisWindow = redisGetWindow(timeBucket, time_divisor);
                    bucket_map = new HashMap<String, Window>();
                    bucket_map.put(campaign_id, redisWindow);
                    campaign_windows.put(timeBucket, bucket_map);
                    return redisWindow;
                }
                // Bucket exists. Check the window.
                Window window = bucket_map.get(campaign_id);
                if (window == null) {
                    // we construct a window by ourselves
                    Window redisWindow = redisGetWindow(timeBucket, time_divisor);
                    bucket_map.put(campaign_id, redisWindow);
                    return redisWindow;
                }
                return window;
            }
        }

    }

}
