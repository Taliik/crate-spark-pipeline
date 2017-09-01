package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static crate.meta.Metadata.*;
import static crate.util.TwitterUtil.prepareTweets;
import static crate.util.TwitterUtil.properties;

public class PredictCrateData {

    public static void main(String[] args) throws IOException, StreamingQueryException, LangDetectException {

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict From Model")
                .master(spark.getProperty("spark.master"))
                .getOrCreate();

        predictCrateData(session);

        session.stop();
    }

    public static void predictCrateData(SparkSession session) throws LangDetectException {

        // read prediction model
        if (Files.notExists(Paths.get(TWITTER_MODEL))) {
            throw new IllegalArgumentException(
                    String.format("Could not find %s.", TWITTER_MODEL)
            );
        }

        // load model
        PipelineModel model = PipelineModel.load(TWITTER_MODEL);

        // fetch data
        Dataset<Row> original = session
                .read()
                .jdbc(
                        properties().getProperty("url"),
                        "(SELECT created_at, id, source, text from tweets) as tweets",
                        properties()
                );

        // ************
        // preparations
        // ************
        Dataset<Row> prepared = prepareTweets(original, 30, true);

        // predict crate data
        Dataset<Row> predicted = model.transform(prepared);



        // write data back to crate
        predicted.select("id", "created_at", TEXT_ORIGINAL, PREDICTION, LABEL_ORIGINAL)
                .write()
                .mode(SaveMode.Append)
                .jdbc(
                        properties().getProperty("url"),
                        "predicted_tweets",
                        properties()

                );
    }
}
