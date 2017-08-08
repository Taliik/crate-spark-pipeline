package crate;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class ConnectionProvider {
    private static Properties properties;
    private final String host;
    private final int httpPort;
    private final String user;

    private CloseableHttpClient httpClient = HttpClients.createSystem();
    private Connection connection;

    private RDD<String> textFile;
    private SparkSession SpS;

    public ConnectionProvider() throws SQLException {
        int psqlPort = Integer.parseInt(getProperty("crate.psql.port"));
        httpPort = Integer.parseInt(getProperty("crate.http.port"));
        host = getProperty("crate.host");
        user = getProperty("crate.user");

        Properties props = new Properties();
        props.setProperty("user", user);

        String url = String.format(Locale.ENGLISH, "jdbc:crate://%s:%d/", host, psqlPort);
        String driver = "io.crate.client.jdbc.CrateDriver";

        try {
            connection = DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            throw new SQLException("Cannot connect to the database", e);
        }

        SpS = SparkSession.builder()
                .master("local[*]")
                .appName("CrateSparkProject")
                .getOrCreate();

        Map<String, String> options = new HashMap<>();
        options.put("driver", driver);
        options.put("url", url);
        options.put("dbtable", "interns.raw_weather");
        options.put("user", user);

        Dataset ds = SpS.sqlContext().load("jdbc", options);
        ds.show();

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
        PreparedStatement prepStat = connection.prepareStatement(
                "SELECT * FROM interns.german_climate_denormalized WHERE temp IS NOT NULL LIMIT 1000"
        );
        ResultSet rs = prepStat.executeQuery();
//        JSONArray json = new JSONArray();
//        ResultSetMetaData rsmd = rs.getMetaData();
//
//        while(rs.next()) {
//            int numColumns = rsmd.getColumnCount();
//            JSONObject obj = new JSONObject();
//            for (int i = 1; i <= numColumns; i++) {
//                String columnName = rsmd.getColumnName(i);
//                obj.put(columnName, rs.getObject(columnName));
//            }
//            json.add(obj);
//        }
//
//        FileWriter writer = new FileWriter("json_array.txt");
//        json.writeJSONString(writer);
//        writer.close();
//
//        return json;
    }



}
