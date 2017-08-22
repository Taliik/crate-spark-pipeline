package crate.app;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.collect.ImmutableMap;
import crate.transformation.RegexTransformator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class App {

    public static void main(String[] args) throws IOException, LangDetectException {

        DetectorFactory.loadProfile("profiles");

        Detector detector = DetectorFactory.create();
        String text = "hello this is a test just to see what language will be picked";
        detector.append(text);
        System.out.println("language of: " + text);
        System.out.println(detector.detect());


        // read properties from resources
        final Properties properties = new Properties();
        properties.load(App.class.getResourceAsStream("/config.properties"));

        System.out.println("used properties:\n");
        System.out.println(properties);

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

        Dataset<Row> cleanData = textCleaner.transform(twitterData);
        cleanData.select("cleanText").collectAsList().forEach(row -> System.out.println(row));


        /*
        apply machine learning

        feed enriched data back into crate

        persist the model somehow - either internal or external
        */
        session.stop();
    }
}