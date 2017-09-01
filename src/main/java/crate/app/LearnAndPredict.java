package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static crate.app.LearnFromTwitter.learnFromTwitter;
import static crate.app.PredictCrateData.predictCrateData;
import static crate.util.TwitterUtil.spark;

public class LearnAndPredict {

    public static void main(String[] args) throws IOException, LangDetectException {

        SparkSession session = SparkSession
                .builder()
                .appName("LearnAndPredictLanguage")
                .master(spark.getProperty("spark.master"))
                .getOrCreate();

        learnFromTwitter(session);
        predictCrateData(session);

        session.stop();

    }
}
