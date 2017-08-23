package crate.transformation;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.LangDetectException;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

import static com.cybozu.labs.langdetect.DetectorFactory.*;

public class LanguageFunction extends AbstractFunction1<String, String> implements Serializable {

    @Override
    public String apply(String s) {
        try {
            Detector detector = create();
            detector.append(s);
            return detector.detect();
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
        return null;
    }
}
