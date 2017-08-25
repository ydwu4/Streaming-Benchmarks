servers="worker1:9093"
for i in {2..30}; do
    servers=$servers",worker"$i":9093";
done

/data/opt/flink-1.3.2/bin/flink run \
-C file:///data/liuzhi/flink-connector-kafka-0.10_2.11-1.3.2.jar \
-C file:///data/liuzhi/flink-connector-kafka-0.9_2.11-1.3.2.jar \
-C file:///data/liuzhi/flink-connector-kafka-base_2.11-1.3.2.jar \
-C file:///data/liuzhi/kafka-clients-0.10.2.1.jar \
-c MainClass \
./target/1.0-alpha.jar \
--bootstrap.servers $servers \
--auto.offset.reset earliest \
--group.id aaaa
