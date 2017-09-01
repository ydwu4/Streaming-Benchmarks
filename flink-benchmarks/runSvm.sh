servers="worker1:9093"
for i in {2..30}; do
    servers=$servers",worker"$i":9093";
done

a9=124

topic=HTK

# remove topic
/data/opt/kafka_2.11-0.11.0.0/bin/kafka-topics.sh --zookeeper master:2182 --delete --topic $topic

/data/opt/flink-1.3.2/bin/flink run \
-C file:///data/liuzhi/flink-connector-kafka-0.10_2.11-1.3.2.jar \
-C file:///data/liuzhi/flink-connector-kafka-0.9_2.11-1.3.2.jar \
-C file:///data/liuzhi/flink-connector-kafka-base_2.11-1.3.2.jar \
-C file:///data/liuzhi/kafka-clients-0.10.2.1.jar \
-C file:///data/opt/flink-1.3.2/opt/flink-ml_2.11-1.3.2.jar \
-c benchmarks.onlinelearning.OnlineSVMExample \
-p 10 \
./target/flink-benchmarks-1.0-SNAPSHOT.jar \
--bootstrap.servers $servers \
--auto.offset.reset earliest \
--data.topic $topic \
--feature.num $a9 \
--learning.rate 0.01 \
--update.frequency 100 \
--regulatization 1
