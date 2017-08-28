package benchmarks.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HDFSToKafka {
    public static Map<String, String> parse(String[] args) {
        Map<String, String> config = new HashMap<String, String>();
        for (int idx = 0; idx < args.length; idx += 2) {
            config.put(args[idx], args[idx + 1]);
        }
        return config;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> config = parse(args);

        String topic = config.get("topic");
        int sleepTime = Integer.valueOf(config.getOrDefault("sleep.time", "2000"));

        Properties kafkaConf = new Properties();
        kafkaConf.put("bootstrap.servers", config.get("bootstrap.servers"));
        kafkaConf.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        kafkaConf.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        kafkaConf.put("client.id", config.getOrDefault("client.id", "HDFSToKafka"));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaConf);

        // Read samples from HDFS.
        // In case of low speed, multiple threads
        // In case of insufficient memory, read again and again instead of cached in memory.
        String host = config.getOrDefault("hdfs.host", "master");
        String port = config.getOrDefault("hdfs.port", "9000");
        Path path = new Path(config.get("hdfs.path"));
        FileSystem fs = FileSystem.get(new URI("hdfs://" + host + ":" + port), new Configuration());
        FileStatus status = fs.getFileStatus(path);
        FSDataInputStream file = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(file), (int) status.getBlockSize());
        ArrayList<String> buffer = new ArrayList<String>();
        for (String line = ""; (line = reader.readLine()) != null; buffer.add(line)) ;
        while (true) {
            for (String line : buffer) {
                producer.send(new ProducerRecord<String, String>(topic, line));
                Thread.sleep(2000);
            }
        }
    }
}