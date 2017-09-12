package crate.meta;

import crate.util.ArgumentParser;
import joptsimple.OptionParser;

import java.util.Arrays;
import java.util.Properties;

/**
 * Contains the most common app data.
 */
public class AppMetadata {

    // connection properties
    public final static String CRATE_HOST = "crate-host";
    public final static String CRATE_HTTP_PORT = "http-port";
    public final static String CRATE_PSQL_PORT = "psql-port";
    public final static String CRATE_USER = "user";
    public final static String CRATE_DRIVER = "driver";

    public final static String CRATE_JDBC_CONNECTION_URL = "jdbc-connection-url";
    public final static String CRATE_BLOB_CONNECTION_URL = "blob-connection-url";

    // model storage properties
    public final static String TABLE_NAME = "spark";
    public final static String MODEL_NAME = "languagepredictor";

    // model column properties
    public final static String TEXT_ORIGINAL = "text";
    public final static String TEXT_FILTERED = "filteredText";
    public final static String TEXT_TOKENIZED = "tokenizedText";
    public final static String TEXT_N_GRAM = "ngrams";
    public final static String TEXT_FEATURED = "features";
    public final static String LABEL_ORIGINAL = "label";
    public final static String LABEL_INDEXED = "labelIndex";
    public final static String PREDICTION_INDEXED = "predictionIndex";
    public final static String PREDICTION = "prediction";
    public final static String PROBABILITY = "probability";

    // default parameters
    public static Properties parse(String[] args) {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("c", CRATE_HOST), "crate-host to connect to e.g. localhost").withRequiredArg().defaultsTo("localhost");
        parser.acceptsAll(Arrays.asList("h", CRATE_HTTP_PORT), "http-port used to connect to crate via http e.g. 4200").withRequiredArg().defaultsTo("4200");
        parser.acceptsAll(Arrays.asList("p", CRATE_PSQL_PORT), "psql-port used to connect to crate via psql e.g. 5432").withRequiredArg().defaultsTo("5432");
        parser.acceptsAll(Arrays.asList("u", CRATE_USER), "crate user for connection e.g. crate").withRequiredArg().defaultsTo("crate");
        parser.acceptsAll(Arrays.asList("d", CRATE_DRIVER), "crate jdbc driver class e.g. io.crate.client.jdbc.CrateDriver").withRequiredArg().defaultsTo("io.crate.client.jdbc.CrateDriver");

        Properties properties = ArgumentParser.parse(args, parser, null);
        properties.setProperty(CRATE_JDBC_CONNECTION_URL, String.format("jdbc:crate://%s:%s/?strict=true", properties.getProperty(CRATE_HOST), properties.getProperty(CRATE_PSQL_PORT)));
        properties.setProperty(CRATE_BLOB_CONNECTION_URL, String.format("http://%s:%s/_blobs/%s/", properties.getProperty(CRATE_HOST), properties.getProperty(CRATE_HTTP_PORT), TABLE_NAME));

        return properties;
    }
}
