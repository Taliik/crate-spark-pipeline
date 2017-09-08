package crate.app;

import crate.util.ArgumentParser;
import joptsimple.OptionParser;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static crate.meta.Metadata.PREDICTION;
import static crate.meta.Metadata.TEXT_FILTERED;

/**
 * PredictLocalUserInput is an application which is launched at a local Spark instance. This is mainly for testing purposes.
 */
public class PredictLocalUserInput {

    public static void main(String[] args) throws IOException {

        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("m", "model-path"), "path of machine learning model used for save/load").withRequiredArg().required();

        Properties properties = ArgumentParser.parse(args, parser, null);

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Predict Local User Input For Testing Purposes")
                .getOrCreate();

        predictUserInput(session, properties);

        session.stop();
    }

    private static void predictUserInput(SparkSession session, Properties properties) throws IOException {
        // load model
        PipelineModel model = PipelineModel.load(properties.getProperty("model-path"));

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
