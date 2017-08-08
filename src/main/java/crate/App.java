package crate;

import java.io.IOException;
import java.sql.SQLException;

import static spark.Spark.port;

public class App {
    public static void main(String[] args) {

        port(Integer.parseInt(ConnectionProvider.getProperty("web.server.port")));

        try {
           ConnectionProvider SparkConnect = new ConnectionProvider();

           SparkConnect.getData();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
