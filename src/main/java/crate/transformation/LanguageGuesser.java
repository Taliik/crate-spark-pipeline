package crate.transformation;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.tools.nsc.Global;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.cybozu.labs.langdetect.DetectorFactory.*;

/**
 * Transformer for language guessing.
 */
public class LanguageGuesser extends IdentifiableUnaryTransformer<String, String, LanguageGuesser> {

    // initialize language detection library
    static {
        try {
            try {
                // load from JAR
                loadProfileFromClassPath();
            } catch (IllegalArgumentException e) {
                // load from IDE
                loadProfile(LanguageGuesser.class.getResource("/profiles").getFile());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
