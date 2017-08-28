package benchmarks.onlinelearning;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.*;

public class DenseVectorSchema implements DeserializationSchema<DenseVector>, SerializationSchema<DenseVector> {
    @Override
    public DenseVector deserialize(byte[] message) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(message));
        double[] values = new double[in.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = in.readDouble();
        }
        return new DenseVector(values);
    }

    @Override
    public byte[] serialize(DenseVector vec) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        try {
            out.writeInt(vec.size());
            for (int i = 0; i < vec.size(); i++) {
                out.writeDouble(vec.data()[i]);
            }
            out.close();
            return bytes.toByteArray();
        } catch (Exception e) {
            return new byte[0];
        }
    }

    @Override
    public TypeInformation<DenseVector> getProducedType() {
        return null; // Don't know how to create the return value
    }

    @Override
    public boolean isEndOfStream(DenseVector nextElement) {
        return false;
    }
}
