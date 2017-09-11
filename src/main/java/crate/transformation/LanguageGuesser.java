package crate.transformation;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.cybozu.labs.langdetect.DetectorFactory.*;

/**
 * Transformer for language guessing.
 */
public class LanguageGuesser extends IdentifiableUnaryTransformer<String, String, LanguageGuesser> {

    private static boolean initializeDetectorFactory = true;

    public LanguageGuesser(String uid) throws LangDetectException, IOException, URISyntaxException {
        getUid(uid);
        init();
    }

    public LanguageGuesser() throws LangDetectException, IOException, URISyntaxException {
        getUid();
        init();
    }

    private void init() throws LangDetectException, IOException, URISyntaxException {
        if (initializeDetectorFactory) {
            initializeDetectorFactory = false;
            try {
                loadProfileFromClassPath();
            } catch (IllegalArgumentException e) {
                loadProfile(LanguageGuesser.class.getResource("/profiles").getFile());
            }
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
