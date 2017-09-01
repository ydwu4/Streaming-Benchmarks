package benchmarks.onlinelearning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.SparseVector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.Serializable;

/**
 * Uncertainty:
 * 1. Whether it's the correct way to train online svm. The way it broadcast and apply gradients here.
 * 2. How to efficiently send gradient from the sink to the source.
 * >> A: Use kafka. Need to delete the topic each time after running the program.
 */
public abstract class OnlineLearningModel implements Serializable {
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
//        DataStream<DenseVector> gradients = env.addSource(new FlinkKafkaConsumer010<String>( // read gradient source from kafka
//            gradTopic,
//            new SimpleStringSchema(),
//            parameterTool.getProperties()
//        )).map(new MapFunction<String, DenseVector>() { // parse string text to gradient vector
//            public DenseVector map(String msg) throws Exception {
//                String[] splits = msg.split(" ");
//                double[] values = new double[Integer.valueOf(splits[0])];
//                for (int i = 0; i < values.length; i++) {
//                    values[i] = Double.valueOf(splits[i + 1]);
//                }
//                return new DenseVector(values);
//            }
//        }).broadcast(); // broadcast the gradient to every physical downstream
        DataStream<DenseVector> gradients = env.addSource(new FlinkKafkaConsumer010<DenseVector>(
            gradTopic,
            new DenseVectorSchema(),
            parameterTool.getProperties()
        )).broadcast();

        env.addSource(new FlinkKafkaConsumer010<String>( // source of samples
            dataTopic,
            new SimpleStringSchema(),
            parameterTool.getProperties()
        )).filter(s -> !s.isEmpty()).map(
            s -> {
                // format of s: label idx1:val1 idx2:val2 idx3:val3 ...
                String[] splits = s.split("\\s");
                double label = formalize(Integer.valueOf(splits[0]));
                int[] indices = new int[splits.length - 1];
                double[] values = new double[splits.length - 1];
                for (int i = 1; i < splits.length; i++) {
                    String[] iv = splits[i].split(":");
                    indices[i - 1] = Integer.valueOf(iv[0]);
                    values[i - 1] = Double.valueOf(iv[1]);
                }
                return new LabeledVector(label, new SparseVector(paramSize, indices, values));
            }
        ).connect(gradients).flatMap(train()).addSink(new FlinkKafkaProducer010<DenseVector>(
            gradTopic,
            new DenseVectorSchema(),
            parameterTool.getProperties()
        ));
//        }).connect(gradients).flatMap(train()).map(new MapFunction<DenseVector, String>() { // parse gradient to string text
//            public String map(DenseVector value) throws Exception {
//                StringBuilder builder = new StringBuilder();
//                builder.append(value.size());
//                for (int i = 0; i < value.size(); i++) {
//                    builder.append(' ');
//                    builder.append(value.data()[i]);
//                }
//                return builder.toString();
//            }
//        }).addSink(new FlinkKafkaProducer010<String>( // send gradient to gradient source
//            gradTopic,
//            new SimpleStringSchema(),
//            parameterTool.getProperties()
//        ));
    }

    protected DenseVector newParams() {
        return new DenseVector(new double[paramSize]);
    }
}
