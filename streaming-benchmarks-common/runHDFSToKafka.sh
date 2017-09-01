servers=worker1:9093
for i in {2..30}; do
    servers=$servers,worker$i:9093
done

datapath=/datasets/classification/a9

topic=HTK

# run program
mvn exec:java \
-Dexec.mainClass="benchmarks.common.HDFSToKafka" \
-Dexec.args="bootstrap.servers $servers hdfs.path $datapath topic $topic send.probability 0.1"

# remove topic
/data/opt/kafka_2.11-0.11.0.0/bin/kafka-topics.sh --zookeeper master:2182 --delete --topic $topic
