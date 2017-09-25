package benchmarks.common;

import java.io.Serializable;

public class Pair<A, B> implements Serializable {
    private A a = null;
    private B b = null;

    public Pair(A a, B b) {
        this.a = a;
        this.b = b;
    }

    public A first() {
        return this.a;
    }

    public B second() {
        return this.b;
    }
}
