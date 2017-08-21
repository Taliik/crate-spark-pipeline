package crate;

import org.apache.spark.ml.feature.NGram;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

public class ConnectionProvider {
    private static Properties properties;
    private final String host = getProperty("crate.host");
    private final int httpPort = Integer.parseInt(getProperty("crate.http.port"));
    private final int psqlPort = Integer.parseInt(getProperty("crate.psql.port"));
    private final String user = getProperty("crate.user");
    private final String driver = "io.crate.client.jdbc.CrateDriver";
    private SparkSession sparkSession;

    public ConnectionProvider() throws SQLException {

        sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CrateSparkProject")
                .getOrCreate();

    }

    static String getProperty(String name) {
        return properties().getProperty(name);
    }

    private static Properties properties() {
        if (properties == null) {
            try {
                properties = new Properties();
                properties.load(ConnectionProvider.class.getResourceAsStream("/config.properties"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return properties;
    }

    public void getData() throws SQLException, IOException {

        String url = String.format(Locale.ENGLISH, "jdbc:crate://%s:%d/", host, psqlPort);

        Dataset<Row> dataset = sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", driver)
                .option("dbtable", "(select text from doc.test_twitter) as tweets")
                .option("user", user)
                .load();

       // List<Row> dataList = dataset.collectAsList();

        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(dataset);

        NGram ngramTransformer = new NGram().setN(3).setInputCol("words").setOutputCol("ngrams");

        Dataset<Row> ngramDataFrame = ngramTransformer.transform(wordsData);
        ngramDataFrame.select("ngrams").show(false);

        sparkSession.stop();
    }


}
