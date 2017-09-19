/data/opt/spark-2.2.0/bin/spark-submit \
--master spark://proj99:7777 \
--jars /home/liuzhi/.ivy2/cache/org.apache.spark/spark-streaming-kafka-0-10_2.11/jars/spark-streaming-kafka-0-10_2.11-2.1.0.jar,\
/home/liuzhi/.ivy2/cache/org.apache.kafka/kafka_2.11/jars/kafka_2.11-0.10.1.0.jar,\
/home/liuzhi/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.10.1.0.jar \
--class "benchmarks.wordcount.WordCount" \
--driver-memory 15g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
target/scala-2.11/sparkbenchmarks_2.11-1.0.jar \
2>&1 | tee log.txt
