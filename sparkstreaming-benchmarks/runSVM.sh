servers=worker1:9093
for i in {2..30}; do
    servers=$servers,worker$i:9093
done

topic=HTK

a9=123

jars=file:///data/zzxx/jars/kafka_2.11-0.11.0.0.jar,\
file:///data/zzxx/jars/kafka-clients-0.11.0.0.jar,\
file:///data/zzxx/jars/spark-streaming-kafka-0-10_2.11-2.2.0.jar

# remove topic
/data/opt/kafka_2.11-0.11.0.0/bin/kafka-topics.sh --zookeeper master:2182 --delete --topic $topic

/data/opt/spark-2.2.0/bin/spark-submit \
--class "benchmarks.onlinelearning.OnlineSVM" \
--jars $jars \
--conf spark.driver.maxResultSize=10g \
--conf spark.metrics.conf=/data/opt/spark-2.2.0/conf/metrics.properties \
--driver-memory 15g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
--master spark://master:7777 \
target/scala-2.11/sparkbenchmarks_2.11-1.0.jar \
bootstrap.servers $servers \
topic $topic \
feature.num $a9 \
label.min -1 \
label.max 1 \
batch.time 1 \
iteration.num 1 \
2>&1 | tee -a log.txt
