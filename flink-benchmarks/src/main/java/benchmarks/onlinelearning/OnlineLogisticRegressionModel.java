package benchmarks.onlinelearning;

import org.apache.flink.ml.common.LabeledVector;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.SparseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class OnlineLogisticRegressionModel extends OnlineLearningModel {
    public OnlineLogisticRegressionModel(String[] args) {
        super(args);
    }

    private static double g(double x) {
        return 1. / (1. + Math.exp(-x));
    }

    private static double h(Vector vec, Vector x) {
        return g(vec.dot(x));
    }

    @Override
    protected CoFlatMapFunction<LabeledVector, DenseVector, DenseVector> train() {
        return new CoFlatMapFunction<LabeledVector, DenseVector, DenseVector>() {
            private DenseVector oldParams = newParams();
            private DenseVector latestParams = newParams();
            private DenseVector accuGradient = newParams();
            private int updateCnt = 0;

            public void flatMap1(LabeledVector v, Collector<DenseVector> coll) throws Exception { // use each sample to train local model
                double fac = v.label() - h(latestParams, v.vector());
                SparseVector features = (SparseVector) v.vector();
                double[] values = features.data();
                int[] indices = features.indices();
                for (int i = 0; i < indices.length; i++) {
                    latestParams.update(indices[i], learningRate * fac * values[i]);
                }
                updateCnt += 1;
                if (updateFreq == updateCnt) { // Can use count based here, not timer based, because we need to ensure coll is legal.
                    updateCnt = 0;
                    DenseVector grad = newParams();
                    for (int i = 0; i < paramSize; i++) {
                        grad.data()[i] = latestParams.data()[i] - oldParams.data()[i];
                    }
                    coll.collect(grad);
                    for (int i = 0; i < paramSize; i++) {
                        oldParams.data()[i] += accuGradient.data()[i];
                        accuGradient.data()[i] = 0;
                    }
                    latestParams = oldParams.copy();
                }
            }

            public void flatMap2(DenseVector grad, Collector<DenseVector> coll) throws Exception { // update local params with broadcasted gradient
                for (int i = 0; i < paramSize; i++) {
                    accuGradient.update(i, accuGradient.data()[i] + grad.data()[i]);
                }
            }
        };
    }
}
