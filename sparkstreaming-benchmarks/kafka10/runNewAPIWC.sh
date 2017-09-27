/data/opt/spark-2.2.0/bin/spark-submit \
--master spark://proj99:7777 \
--jars /home/hduser/.ivy2/cache/org.apache.spark/spark-streaming-kafka-0-10_2.11/jars/spark-streaming-kafka-0-10_2.11-2.1.0.jar,\
/home/hduser/.ivy2/cache/org.apache.kafka/kafka_2.11/jars/kafka_2.11-0.11.0.0.jar,\
/home/hduser/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.11.0.0.jar,\
/home/hduser/.ivy2/cache/net.sf.jopt-simple/jopt-simple/jars/jopt-simple-5.0.4.jar \
--class "benchmarks.wordcount.WordCountByNewAPI" \
--driver-memory 15g \
--driver-cores 4 \
--executor-memory 10g \
--executor-cores 6 \
target/scala-2.11/sparkbenchmarks_2.11-1.0.jar \
"$@"
