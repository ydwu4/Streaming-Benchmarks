
# run program
sbt "run-main benchmarks.common.KafkaWordCountProducer --prob $prob --zookeeper master:2182  --consumer-topic $consumer_topic --producer-topic $producer_topic --broker-list $brokers --delete-consumer-offsets --from-beginning"
