package crate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static spark.Spark.port;

public class App {
    public static void main(String[] args) throws InterruptedException, IOException {

        //Start a crate instance here
//        String homeDir = System.getProperty("user.home") + "/IdeaProjects/crate.io/crate-spark-pipeline/crate-2.1.5/bin";
//        Process process;
//
//        process = Runtime.getRuntime().exec(String.format("%s/crate", homeDir));


        TimeUnit.SECONDS.sleep(10);

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
