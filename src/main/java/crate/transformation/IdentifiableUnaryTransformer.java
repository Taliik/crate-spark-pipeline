package crate.transformation;

import org.apache.spark.ml.UnaryTransformer;
import org.apache.spark.ml.util.Identifiable$;

import java.io.Serializable;

public abstract class IdentifiableUnaryTransformer<IN, OUT, T extends IdentifiableUnaryTransformer<IN, OUT, T>> extends UnaryTransformer<IN, OUT, T> implements Serializable {

    private String uid;

    @Override
    public String uid() {
        return getUid();
    }

    protected String getUid() {
        if (uid == null) {
            uid = Identifiable$.MODULE$.randomUID(getName());
        }
        return uid;
    }

    protected String getUid(String value) {
        if (uid == null) {
            uid = value;
        }
        return uid;
    }

    public abstract String getName();
}
