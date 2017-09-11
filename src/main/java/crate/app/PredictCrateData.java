package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import static crate.meta.Metadata.*;
import static crate.util.ArgumentParser.parse;
import static crate.util.CrateBlobRepository.load;
import static crate.util.TwitterUtil.prepareTweets;

/**
 * PredictCrateData loads a language prediction model from a CrateDB BLOB table to predict all currently imported Tweets from CrateDB and stores the new data with predictions in a new `predicted_tweets` table.
 */
public class PredictCrateData {

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, URISyntaxException, LangDetectException {
        // load properties
        Properties properties = parse(args);

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict From Model")
                .getOrCreate();

        // load model from CrateDB
        Transformer model = (Transformer) load(properties, MODEL_NAME);

        // apply predictions
        predictCrateData(session, properties, model);

        session.stop();
    }

    public static void predictCrateData(SparkSession session, Properties properties, Transformer model) throws IOException, LangDetectException, URISyntaxException {
        // fetch data
        Dataset<Row> original = session
                .read()
                .jdbc(
                        properties.getProperty(CRATE_JDBC_CONNECTION_URL),
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
                        properties.getProperty(CRATE_JDBC_CONNECTION_URL),
                        "predicted_tweets",
                        properties
                );
    }
}
