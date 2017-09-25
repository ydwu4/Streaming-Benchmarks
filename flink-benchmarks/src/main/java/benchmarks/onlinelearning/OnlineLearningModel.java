package benchmarks.onlinelearning;

import benchmarks.common.Pair;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.SparseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Uncertainty:
 * 1. Whether it's the correct way to train online svm. The way it broadcast and apply gradients here.
 * 2. How to efficiently send gradient from the sink to the source.
 * >> A: Use kafka. Need to delete the topic each time after running the program.
 */
public abstract class OnlineLearningModel implements Serializable {
    private Logger LOG = LoggerFactory.getLogger(OnlineSVMModel.class);

    protected ParameterTool parameterTool;
    protected String dataTopic;
    protected int paramSize;
    protected double learningRate;
    protected int updateFreq;
    protected double regularization;
    protected String gradTopic;

    private static double formalize(double label) {
        if (label == 1) {
            return 1;
        } else {
            return -1;
        }
    }

    protected abstract CoFlatMapFunction<LabeledVector, DenseVector, DenseVector> train();

    public OnlineLearningModel(String[] args) {
        parameterTool = ParameterTool.fromArgs(args);
        dataTopic = parameterTool.get("data.topic");
        paramSize = parameterTool.getInt("feature.num");
        learningRate = parameterTool.getDouble("learning.rate", 0.01);
        updateFreq = parameterTool.getInt("update.frequency", 100);
        regularization = parameterTool.getDouble("regularization", 1);
        gradTopic = parameterTool.get("grad.topic", "online-svm-grad");
    }

    public void modeling(StreamExecutionEnvironment env) {
        DataStream<DenseVector> gradients = env.addSource(new FlinkKafkaConsumer010<DenseVector>(
            gradTopic,
            new DenseVectorSchema(),
            parameterTool.getProperties()
        )).broadcast();

        DataStream<Pair<Long, LabeledVector>> input = env.addSource(new FlinkKafkaConsumer010<String>( // source of samples
            dataTopic,
            new SimpleStringSchema(),
            parameterTool.getProperties()
        )).filter(s -> !s.isEmpty()).map(
            s -> {
                // format of s: timestamp sample
                // format of sample: label idx1:val1 idx2:val2 idx3:val3 ...
                String[] splits = s.split("\\s");
                long time = Integer.valueOf(splits[0]);
                double label = formalize(Integer.valueOf(splits[1]));

                int[] indices = new int[splits.length - 1];
                double[] values = new double[splits.length - 1];
                for (int i = 2; i < splits.length; i++) {
                    String[] iv = splits[i].split(":");
                    indices[i - 2] = Integer.valueOf(iv[0]);
                    values[i - 2] = Double.valueOf(iv[1]);
                }
                return new Pair<>(time, new LabeledVector(label, new SparseVector(paramSize, indices, values)));
            }
        );

        input.map(Pair::second).connect(gradients).flatMap(train()).addSink(new FlinkKafkaProducer010<DenseVector>(
            gradTopic,
            new DenseVectorSchema(),
            parameterTool.getProperties()
        ));

        int metricInterval = parameterTool.getInt("metric.interval", 1);
        input.map(Pair::first)
            .map(tm -> System.currentTimeMillis() - tm)
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(metricInterval)))
            .aggregate(new AggregateFunction<Long, LatencyThroughputAccumulator, Pair>() {
                @Override
                public LatencyThroughputAccumulator createAccumulator() {
                    return new LatencyThroughputAccumulator(metricInterval);
                }

                @Override
                public void add(Long value, LatencyThroughputAccumulator accumulator) {
                    accumulator.add(value);
                }

                @Override
                public Pair getResult(LatencyThroughputAccumulator accumulator) {
                    return accumulator.getResult();
                }

                @Override
                public LatencyThroughputAccumulator merge(LatencyThroughputAccumulator a, LatencyThroughputAccumulator b) {
                    return LatencyThroughputAccumulator.merge(a, b);
                }
            }).addSink((SinkFunction<Pair>) value -> {
            LOG.info("Latency: {} ms, Throughput: {} rec/sec", value.first(), value.second());
        });
    }

    protected DenseVector newParams() {
        return new DenseVector(new double[paramSize]);
    }
}

class LatencyThroughputAccumulator {
    private long count = 0;
    private long sum = 0;
    private double interval;

    public LatencyThroughputAccumulator(double interval) {
        this.interval = interval;
    }

    public void add(Long latency) {
        sum += latency;
        count += 1;
    }

    public Pair getResult() {
        return new Pair<>((sum + 0.) / count, count / interval);
    }

    static public LatencyThroughputAccumulator merge(LatencyThroughputAccumulator a, LatencyThroughputAccumulator b) {
        LatencyThroughputAccumulator acc = new LatencyThroughputAccumulator(a.interval);
        acc.count = a.count + b.count;
        acc.sum = a.sum + b.sum;
        return acc;
    }
}