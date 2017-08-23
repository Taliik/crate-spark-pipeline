package crate.app;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.collect.ImmutableMap;
import crate.transformation.LanguageTransformator;
import crate.transformation.RegexTransformator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;

public class App {

    public static void main(String[] args) throws IOException, LangDetectException {
        // read properties from resources
        final Properties properties = new Properties();
        properties.load(App.class.getResourceAsStream("/config.properties"));

        System.out.println("used properties:\n");
        System.out.println(properties);

        //initialize language detector factory
        DetectorFactory.loadProfile(properties.getProperty("language.profiles.path"));

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName(properties.getProperty("spark.app.name"))
                .master(properties.getProperty("spark.master"))
                .getOrCreate();

        //fetch data
        final String url = String.format(Locale.ENGLISH,
                "jdbc:crate://%s:%s/",
                properties.getProperty("crate.host"),
                properties.getProperty("crate.psql.port"));

        Map<String, String> options = ImmutableMap
                .<String, String>builder()
                .put("url", url)
                .put("driver", properties.getProperty("crate.driver"))
                .put("user", properties.getProperty("crate.user"))
                .put("dbtable", properties.getProperty("crate.table"))
                .build();

        Dataset<Row> twitterData = session
                .read()
                .format("jdbc")
                .options(options)
                .load();

        final String transformationPattern =
                "(&\\w+;)"
                        //retweets
                        + "|(^RT @\\w+: )"
                        //emails
                        + "|(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])" //e-mails
                        //user-tags
                        + "|(@\\w+)"
                        //links
                        + "|((https?|ftp):\\/\\/[^\\s/$.?#].[^\\s]*)"
                        //hash-tags
                        + "|(#\\w+)"
                        //emojis
                        + "|([\\u203C-\\uDFFF])";

        // apply transformations
        RegexTransformator textCleaner = new RegexTransformator(transformationPattern, "")
                .setInputCol("text")
                .setOutputCol("cleanText");

        LanguageTransformator languageGuesser = new LanguageTransformator()
                .setInputCol("cleanText")
                .setOutputCol("label");

        // clean the dataset using the regular expression above
        Dataset<Row> cleanData = textCleaner.transform(twitterData);

        // remove tweets which are shorter than 50 characters -> language guess is quite bad there
        Dataset<Row> preFiltered = cleanData.filter(length(col("cleanText")).geq(50));

        // label remaining tweets
        Dataset<Row> labeledData = languageGuesser.transform(preFiltered);

        // drop the tweets which could not be guessed (should be near 0)
        Dataset<Row> result = labeledData
                .filter(col("label").isNotNull())
                .filter(col("label").notEqual("unknown"));


        // for test: display labeled tweets
        result
                .select("label", "cleanText")
                .collectAsList()
                .forEach(row -> System.out.println(row));

        // TODO: apply machine learning, feed enriched data back into crate, persist the model
        // apply machine learning
        // feed enriched data back into crate
        // persist the model somehow - either internal or external

        session.stop();
    }
}