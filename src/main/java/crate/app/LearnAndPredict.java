package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
import crate.util.ArgumentParser;
import joptsimple.OptionParser;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;

import static crate.app.LearnFromTwitter.learnFromTwitter;
import static crate.app.PredictCrateData.predictCrateData;

public class LearnAndPredict {

    public static void main(String[] args) throws IOException, LangDetectException, URISyntaxException {

        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("c", "connection-url"), "crate host to connect to e.g. jdbc:crate://localhost:5432/?strict=true").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("u", "user"), "crate user for connection e.g. crate").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("d", "driver"), "crate jdbc driver class").withRequiredArg().defaultsTo("io.crate.client.jdbc.CrateDriver");
        parser.acceptsAll(Arrays.asList("m", "model-path"), "path of machine learning model used for save/load").withRequiredArg().required();

        Properties properties = ArgumentParser.parse(args, parser, null);

        SparkSession session = SparkSession
                .builder()
                .appName("LearnAndPredictLanguage")
                .getOrCreate();

        learnFromTwitter(session, properties);
        predictCrateData(session, properties);

        session.stop();

    }
}
