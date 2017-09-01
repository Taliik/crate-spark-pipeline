package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
import crate.util.ArgumentParser;
import joptsimple.OptionParser;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import static crate.meta.Metadata.*;
import static crate.util.TwitterUtil.prepareTweets;

public class PredictCrateData {

    public static void main(String[] args) throws IOException, LangDetectException {

        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("c", "connection-url"), "crate host to connect to e.g. jdbc:crate://localhost:5432/?strict=true").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("u", "user"), "crate user for connection e.g. crate").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("d", "driver"), "crate jdbc driver class").withRequiredArg().defaultsTo("io.crate.client.jdbc.CrateDriver");

        Properties properties = ArgumentParser.parse(args, parser, null);

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict From Model")
                .getOrCreate();

        predictCrateData(session, properties);

        session.stop();
    }

    public static void predictCrateData(SparkSession session, Properties properties) throws IOException, LangDetectException {

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
                        properties.getProperty("connection-url"),
                        "(SELECT created_at, id, source, text from tweets) as tweets",
                        properties
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
                        properties.getProperty("connection-url"),
                        "predicted_tweets",
                        properties
                );
    }
}
