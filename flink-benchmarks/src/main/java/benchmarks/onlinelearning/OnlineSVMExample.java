package benchmarks.onlinelearning;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OnlineSVMExample {
    static public void main(String[] args) throws Exception {
        OnlineSVMModel svm = new OnlineSVMModel(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        svm.modeling(env);
        env.execute("online svm alpha");
    }
}
