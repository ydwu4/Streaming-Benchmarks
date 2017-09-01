package benchmarks.onlinelearning;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OnlineLogisticRegressionExample {
    static public void main(String[] args) throws Exception {
        OnlineLogisticRegressionModel log = new OnlineLogisticRegressionModel(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        log.modeling(env);
        env.execute("online log reg alpha");
    }
}
