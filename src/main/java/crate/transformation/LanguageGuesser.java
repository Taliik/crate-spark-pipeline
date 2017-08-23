package crate.transformation;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;

import static com.cybozu.labs.langdetect.DetectorFactory.create;

public class LanguageGuesser extends IdentifiableUnaryTransformer<String, String, LanguageGuesser> {

    public LanguageGuesser(String uid) {
        getUid(uid);
    }

    public LanguageGuesser() {
        getUid();
    }

    @Override
    public Function1<String, String> createTransformFunc() {
        return new SerializableAbstractFunction<String, String>() {
            @Override
            public String apply(String s) {
                try {
                    Detector detector = create();
                    detector.append(s);
                    return detector.detect();
                } catch (LangDetectException e) {
                    return "unknown";
                }
            }
        };
    }

    @Override
    public DataType outputDataType() {
        return DataTypes.StringType;
    }

    @Override
    public String getName() {
        return "languageguesser";
    }
}
