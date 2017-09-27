if [ "$#" -ne 1 ]; then
    echo "Usage: topic"
    exit 1
fi

topic=$1

cd /data/opt/kafka_2.11-0.11.0.0/bin
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list worker1:9093,worker2:9093,worker3:9093,worker4:9093,worker5:9093,worker6:9093,worker7:9093,worker8:9093,worker9:9093,worker10:9093,worker11:9093,worker12:9093,worker13:9093,worker14:9093,worker15:9093,worker16:9093,worker17:9093,worker18:9093,worker19:9093,worker20:9093,worker21:9093,worker22:9093,worker23:9093,worker24:9093,worker25:9093,worker26:9093,worker27:9093,worker28:9093,worker29:9093,worker30:9093 --topic $topic --time -1 | awk -F ':' '{print $3}' | awk '{s+=$1} END {print s}'
