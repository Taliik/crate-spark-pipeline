package crate.app;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static crate.meta.Metadata.*;

public class PredictUserInput {

    public static void main(String[] args) throws IOException {

        // read prediction model
        if (Files.notExists(Paths.get(TWITTER_MODEL))) {
            throw new IllegalArgumentException(
                    String.format("Could not find model at %s.", TWITTER_MODEL)
            );
        }

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict User Input From Model")
                .getOrCreate();

        // load model
        PipelineModel model = PipelineModel.load(TWITTER_MODEL);

        // predict user input data
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        String line;

        while (!(line = input.readLine()).isEmpty()) {
            // create dataframe from input
            List<Row> data = Arrays.asList(
                    RowFactory.create(line)
            );
            StructType schema = new StructType(new StructField[]{
                    DataTypes.createStructField(TEXT_FILTERED, DataTypes.StringType, false)
            });
            Dataset<Row> df = session.createDataFrame(data, schema);

            // apply predictions on data
            Dataset<Row> predictions = model.transform(df);

            // print results
            predictions.select(TEXT_FILTERED, PREDICTION).collectAsList().forEach(row -> System.out.println(row));
        }

        session.stop();
    }
}
