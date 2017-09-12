package crate.transformation;

import org.apache.spark.ml.UnaryTransformer;
import org.apache.spark.ml.util.Identifiable$;

import java.io.Serializable;

/**
 * This class solves the uid issues which occur when implementing a UnaryTransformer in Java.
 *
 * @param <IN> type that goes in as parameter
 * @param <OUT> type that goes out as return type
 * @param <T> the class itself
 */
public abstract class IdentifiableUnaryTransformer<IN, OUT, T extends IdentifiableUnaryTransformer<IN, OUT, T>> extends UnaryTransformer<IN, OUT, T> implements Serializable {

    private String uid;

    protected IdentifiableUnaryTransformer() {
        getUid();
    }

    @Override
    public String uid() {
        return getUid();
    }

    private String getUid() {
        if (uid == null) {
            uid = Identifiable$.MODULE$.randomUID(getName());
        }
        return uid;
    }

    public abstract String getName();
}
