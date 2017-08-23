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
import static crate.util.TwitterUtil.*;

public class PredictCrateData {

    public static void main(String[] args) throws IOException, StreamingQueryException, LangDetectException {

        // read prediction model
        String modelFileName = spark.getProperty("spark.model");
        if (Files.notExists(Paths.get(modelFileName))) {
            throw new IllegalArgumentException(
                    String.format("Could not find model at %s.", modelFileName)
            );
        }

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict From Model")
                .master(spark.getProperty("spark.master"))
                .getOrCreate();

        // load model
        PipelineModel model = PipelineModel.load(modelFileName);

        // fetch data
        Dataset<Row> original = session
                .read()
                .jdbc(
                        crate.getProperty("url"),
                        "(SELECT created_at, id, source, text from tweets) as tweets",
                        crate
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
                        crate.getProperty("url"),
                        "predicted_tweets",
                        crate
                );

        session.stop();
    }
}
