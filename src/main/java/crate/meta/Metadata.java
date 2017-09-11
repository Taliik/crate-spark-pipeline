package crate.meta;

public class Metadata {

    public final static String CRATE_HOST = "crate-host";
    public final static String CRATE_HTTP_PORT = "http-port";
    public final static String CRATE_PSQL_PORT = "psql-port";
    public final static String CRATE_USER = "user";
    public final static String CRATE_DRIVER = "driver";

    public final static String CRATE_JDBC_CONNECTION_URL = "jdbc-connection-url";
    public final static String CRATE_BLOB_CONNECTION_URL = "blob-connection-url";

    public final static String TABLE_NAME = "spark";
    public final static String MODEL_NAME = "languagepredictor";

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
}
