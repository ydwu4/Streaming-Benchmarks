mvn exec:java -Dexec.mainClass="benchmarks.common.Main" -Dexec.args="--bootstrap.servers worker1:9093,worker2:9093 --group.id abc123 --consumer_topic twitter --producer_topic transfer"
