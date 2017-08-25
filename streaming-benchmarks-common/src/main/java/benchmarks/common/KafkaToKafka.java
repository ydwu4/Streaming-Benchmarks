package benchmarks.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaToKafka {

    public static void main(final String[] args) throws Exception {

        ParameterTool paratool = ParameterTool.fromArgs(args);
        //----------------------------------------------------- consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", paratool.getRequired("bootstrap.servers"));
        props.put("group.id", paratool.getRequired("group.id"));
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> topics = new ArrayList<>();
        topics.add(paratool.getRequired("consumer_topic"));
        consumer.subscribe(topics);
        //----------------------------------------------------- consumer

        //----------------------------------------------------- producer
        Properties pd_props = new Properties();
        pd_props.put("bootstrap.servers", paratool.getRequired("bootstrap.servers"));
        pd_props.put("acks", "all");
        pd_props.put("retries", 0);
        pd_props.put("batch.size", 16384);
        pd_props.put("linger.ms", 1);
        pd_props.put("buffer.memory", 33554432);
        pd_props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pd_props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String pd_topic = paratool.getRequired("producer_topic");
        Producer<String, String> producer = new KafkaProducer<>(pd_props);
        //----------------------------------------------------- producer

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            int i = 1;
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("" + i);
                i++;
            }
            i = 1;
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("" + i);
                producer.send(new ProducerRecord<String, String>(pd_topic, record.key(), record.value()));
                i++;
            }

        }

        //producer.close();
    }

}
