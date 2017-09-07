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
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;

import static crate.meta.Metadata.*;
import static crate.util.TwitterUtil.prepareTweets;

public class PredictCrateData {

    public static void main(String[] args) throws IOException, LangDetectException, URISyntaxException {

        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("c", "connection-url"), "crate host to connect to e.g. jdbc:crate://localhost:5432/?strict=true").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("u", "user"), "crate user for connection e.g. crate").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("d", "driver"), "crate jdbc driver class").withRequiredArg().defaultsTo("io.crate.client.jdbc.CrateDriver");
        parser.acceptsAll(Arrays.asList("m", "model-path"), "path of machine learning model used for save/load").withRequiredArg().required();

        Properties properties = ArgumentParser.parse(args, parser, null);

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict From Model")
                .getOrCreate();

        predictCrateData(session, properties);

        session.stop();
    }

    public static void predictCrateData(SparkSession session, Properties properties) throws IOException, LangDetectException, URISyntaxException {

        // load model
        PipelineModel model = PipelineModel.load(properties.getProperty("model-path"));

        // fetch data
        Dataset<Row> original = session
                .read()
                .jdbc(
                        properties.getProperty("connection-url"),
                        "(SELECT t.id, t.created_at, t.text from tweets t left join predicted_tweets p on t.id = p.id where p.id is null) as tweets",
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
