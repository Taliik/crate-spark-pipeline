package crate.transformation;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.sun.istack.internal.NotNull;
import org.apache.spark.ml.UnaryTransformer;
import org.apache.spark.ml.util.Identifiable$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;

public class LanguageTransformator extends UnaryTransformer<String, String, LanguageTransformator> {

    private String uid;

    @Override
    public Function1<String, String> createTransformFunc() {
        return new LanguageFunction();
    }

    @Override
    public DataType outputDataType() {
        return DataTypes.StringType;
    }

    @Override
    public String uid() {
        if (uid == null) {
            uid = Identifiable$.MODULE$.randomUID("languageapplier");
        }
        return uid;
    }
}