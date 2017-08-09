package crate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

public class ConnectionProvider {
    private static Properties properties;
    private final String host;
    private final int httpPort;
    private final String user;

    //private CloseableHttpClient httpClient = HttpClients.createSystem();
    private Connection connection;
    private SparkSession SpS;

    public ConnectionProvider() throws SQLException {
        int psqlPort = Integer.parseInt(getProperty("crate.psql.port"));
        httpPort = Integer.parseInt(getProperty("crate.http.port"));
        host = getProperty("crate.host");
        user = getProperty("crate.user");

        String url = String.format(Locale.ENGLISH, "jdbc:crate://%s:%d/", host, psqlPort);
        String driver = "io.crate.client.jdbc.CrateDriver";

        SpS = SparkSession.builder()
                .master("local[*]")
                .appName("CrateSparkProject")
                .getOrCreate();

        Dataset<Row> dataset = SpS.read()
                .format("jdbc")
                .option("url", url)
                .option("driver", driver)
                .option("dbtable", "(select * from interns.raw_weather where date>'2013-01-01T03:59:00') as weather_data")
                .option("user", user)
                .load();

        System.out.println(dataset.count());
        dataset.show();

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
    }



}
