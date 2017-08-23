package crate.transformation;

import com.sun.istack.internal.NotNull;
import org.apache.spark.ml.UnaryTransformer;
import org.apache.spark.ml.util.Identifiable$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;

public class RegexTransformator extends UnaryTransformer<String, String, RegexTransformator> {

    private String uid;
    private final String pattern;
    private final String replacement;

    public RegexTransformator(@NotNull String pattern, @NotNull String replacement) {
        this.pattern = pattern;
        this.replacement = replacement;
    }


    @Override
    public Function1<String, String> createTransformFunc() {
        return new RegexFunction(pattern, replacement);
    }

    @Override
    public DataType outputDataType() {
        return DataTypes.StringType;
    }

    @Override
    public String uid() {
        if (uid == null) {
            uid = Identifiable$.MODULE$.randomUID("regexapplier");
        }
        return uid;
    }
}