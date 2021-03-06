servers=worker1:9093
for i in {2..30}; do
    servers=$servers,worker$i:9093
done

topic=HTK

a9=124
kddb=29890096

jars=file:///data/zzxx/jars/kafka_2.11-0.11.0.0.jar,\
file:///data/zzxx/jars/kafka-clients-0.11.0.0.jar,\
file:///data/zzxx/jars/spark-streaming-kafka-0-10_2.11-2.2.0.jar

user=$(whoami)
mkdir -p /data/$user/spark-logs/
LOGFILE=/data/$user/spark-logs/OLLR-$(date +%s).cilent

# remove topic
/data/opt/kafka_2.11-0.11.0.0/bin/kafka-topics.sh --zookeeper master:2182 --delete --topic $topic

# add this for metric configuration system
# --conf spark.metrics.conf=/data/opt/spark-2.2.0/conf/metrics.properties \

/data/opt/spark-2.2.0/bin/spark-submit \
--class "benchmarks.onlinelearning.OnlineLogisticRegression" \
--jars $jars \
--conf spark.driver.maxResultSize=10g \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--driver-memory 15g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
--master spark://proj99:7777 \
target/scala-2.11/sparkbenchmarks_2.11-1.0.jar \
bootstrap.servers $servers \
topic $topic \
feature.num $kddb \
label.min -1 \
label.max 1 \
iteration.num 1 \
batch.time 120 \
2>&1 | tee -a $LOGFILE
