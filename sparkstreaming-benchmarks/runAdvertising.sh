brokers="worker1"
for i in {2..30}; do
    brokers="$brokers,worker$i"
done

jars=file:///data/zzxx/jars/kafka_2.11-0.11.0.0.jar,\
file:///data/zzxx/jars/kafka-clients-0.11.0.0.jar,\
file:///data/zzxx/jars/sedis_2.11-1.2.2.jar,\
file:///data/zzxx/jars/jedis-2.9.0.jar,\
file:///data/zzxx/jars/commons-pool2-2.4.2.jar,\
file:///data/zzxx/jars/spark-sql-kafka-0-10_2.11-2.2.0.jar,\
file:///data/zzxx/jars/json-20170516.jar

user=$(whoami)
mkdir -p /data/$user/spark-logs/
LOGFILE=/data/$user/spark-logs/AD-$(date +%s).cilent

# add this for metric configuration system
# --conf spark.metrics.conf=/data/opt/spark-2.2.0/conf/metrics.properties \

/data/opt/spark-2.2.0/bin/spark-submit \
--class "benchmarks.advertising.AdvertisingSql" \
--name "AdvertisingSql" \
--jars $jars \
--conf spark.driver.maxResultSize=10g \
--driver-memory 15g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
--master spark://proj99:7777 \
target/scala-2.11/sparkbenchmarks_2.11-1.0.jar \
redis.host worker21 \
kafka.port 9093 \
kafka.brokers $brokers \
kafka.topic ad-events \
2>&1 | tee -a $LOGFILE
