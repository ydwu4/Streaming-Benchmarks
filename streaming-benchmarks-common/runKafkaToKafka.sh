brokers=worker1:9093
for i in {2..30}; do
	brokers=$brokers,worker$i:9093
done

if [ "$#" -ne 3 ]; then
	echo "Usage: cosumer_topic producer_topic send_probability"
	exit 1
fi

consumer_topic=$1

producer_topic=$2

prob=$3

# run program
sbt "run-main benchmarks.common.KafkaToKafka --prob $prob --zookeeper master:2182  --consumer-topic $consumer_topic --producer-topic $producer_topic --broker-list $brokers --delete-consumer-offsets"

#remove topic
#/data/opt/kafka_2.11-0.11.0.0/bin/kafka-topics.sh --zookeeper master:2182 --delete --topic $producer_topic
