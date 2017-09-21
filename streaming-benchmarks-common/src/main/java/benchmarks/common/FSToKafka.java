package benchmarks.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class FSToKafka {
    public static Map<String, String> parse(String[] args) {
        Map<String, String> config = new HashMap<String, String>();
        for (int idx = 0; idx < args.length; idx += 2) {
            config.put(args[idx], args[idx + 1]);
        }
        return config;
    }

    static public void main(String[] args) throws Exception {
        Map<String, String> config = parse(args);

        String topic = config.get("topic");
        double sendProb = Double.valueOf(config.getOrDefault("send.probability", "1"));

        Properties kafkaConf = new Properties();
        kafkaConf.put("bootstrap.servers", config.get("bootstrap.servers"));
        kafkaConf.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        kafkaConf.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        kafkaConf.put("client.id", config.getOrDefault("client.id", "HDFSToKafka"));

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaConf);

        File file = new File(config.get("file.path"));
        if (!file.exists()) {
            throw new Exception("File " + file + " does not exist.");
        }
        Random rand = new Random();
        final HDFSToKafka.Record rec = new HDFSToKafka.Record();
        new Thread(() -> {
            while (true) {
                int curSend = rec.numSend;
                System.out.println("Throughput (rec/sec): " + (curSend - rec.lastSend));
                rec.lastSend = curSend;
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
            }
        }).start();

        while (true) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (rand.nextDouble() < sendProb) {
                    rec.numSend += 1;
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, line);
                    producer.send(record);
                }
            }
            reader.close();
        }
    }
}
