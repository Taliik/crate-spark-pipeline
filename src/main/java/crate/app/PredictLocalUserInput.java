package crate.app;

import org.apache.spark.ml.Transformer;
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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static crate.meta.AppMetadata.*;
import static crate.util.CrateBlobStorageUtil.load;

/**
 * PredictLocalUserInput is an application which is launched at a local Spark instance. This is mainly for testing purposes. A model instance is loaded from a CrateDB BLOB table.
 */
public class PredictLocalUserInput {

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        // load properties
        Properties properties = parse(args);

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict Local User Input For Testing Purposes")
                .getOrCreate();

        // load model from CrateDB
        Transformer transformer = (Transformer) load(properties, TABLE_NAME, MODEL_NAME);

        predictUserInput(session, properties, transformer);

        session.stop();
    }

    private static void predictUserInput(SparkSession session, Properties properties, Transformer model) throws IOException {

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
    }
}
