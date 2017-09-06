package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
import crate.util.ArgumentParser;
import joptsimple.OptionParser;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;

import static crate.meta.Metadata.*;
import static crate.util.TwitterUtil.prepareTweets;

public class LearnAndPredictRemote {

    public static void main(String[] args) throws IOException, LangDetectException, URISyntaxException {

        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("c", "connection-url"), "crate host to connect to e.g. jdbc:crate://localhost:5432/?strict=true").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("u", "user"), "crate user for connection e.g. crate").withRequiredArg().required();
        parser.acceptsAll(Arrays.asList("d", "driver"), "crate jdbc driver class").withRequiredArg().defaultsTo("io.crate.client.jdbc.CrateDriver");

        Properties properties = ArgumentParser.parse(args, parser, null);

        SparkSession session = SparkSession
                .builder()
                .appName("LearnAndPredictLanguage")
                .getOrCreate();

        predictCrateData(session, properties, learnFromTwitter(session, properties));

        session.stop();

    }

    public static PipelineModel learnFromTwitter(SparkSession session, Properties properties) throws IOException, LangDetectException, URISyntaxException {

        // fetch data
        Dataset<Row> original = session
                .read()
                .jdbc(
                        properties.getProperty("connection-url"),
                        "(SELECT text from tweets) as tweets",
                        properties
                );

        // ************
        // preparations
        // ************

        Dataset<Row> rawLabeled = prepareTweets(original, 30, true);

        // label indexing
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol(LABEL_ORIGINAL)
                .setOutputCol(LABEL_INDEXED)
                .setHandleInvalid("keep")
                .fit(rawLabeled);

        Dataset<Row> prepared = labelIndexer.transform(rawLabeled);

        // ******************************
        // pipeline components start here
        // ******************************

        // tokenize - split each sentence into an array of lowercase characters (strings with length 1)
        RegexTokenizer tokenizer = new RegexTokenizer()
                .setInputCol(TEXT_FILTERED)
                .setOutputCol(TEXT_TOKENIZED)
                .setGaps(false)
                .setToLowercase(true)
                .setPattern(".");

        // n-gram - build n-grams on character basis of length n to recognize language patterns
        NGram ngram = new NGram()
                .setInputCol(TEXT_TOKENIZED)
                .setOutputCol(TEXT_N_GRAM);

        // hashing-tf - convert text to calculatable numbers
        HashingTF featurizer = new HashingTF()
                .setInputCol(TEXT_N_GRAM)
                .setOutputCol(TEXT_FEATURED);

        // machine learning algorithm
        NaiveBayes machineLearningAlgorithm = new NaiveBayes()
                .setFeaturesCol(TEXT_FEATURED)
                .setLabelCol(LABEL_INDEXED)
                .setPredictionCol(PREDICTION_INDEXED)
                .setProbabilityCol(PROBABILITY);

        // convert prediction indexes back into human readable language guesses
        // here we need to pass the labels which were used for indexing
        IndexToString predictionToLabel = new IndexToString()
                .setInputCol(PREDICTION_INDEXED)
                .setOutputCol(PREDICTION)
                .setLabels(labelIndexer.labels());

        // THE ORDER OF THE PIPELINE
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                tokenizer,
                ngram,
                featurizer,
                machineLearningAlgorithm,
                predictionToLabel
        });

        // ******************
        // pipeline ends here
        // ******************

        // ***********************
        // PIPELINE TUNING SECTION
        // ***********************
        // in this section you can set possible values used by the different transformers
        // keep in mind that every possible combination of these parameters is executed for the whole dataset.
        // WARNING: testing every possible combination is a very expensive operation, keep parameters as small as possible!!

        ParamMap[] paramMaps = new ParamGridBuilder()
                .addGrid(ngram.n(), new int[]{3, 4})
                .addGrid(featurizer.numFeatures(), new int[]{20000, 25000, 30000})
                .build();

        // used for evaluation of multiclassification problems
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol(LABEL_INDEXED)
                .setPredictionCol(PREDICTION_INDEXED);

        CrossValidator validator = new CrossValidator()
                .setEstimator(pipeline)
                .setEstimatorParamMaps(paramMaps)
                .setEvaluator(evaluator)
                .setNumFolds(2);

        // find best model
        PipelineModel model = (PipelineModel) validator.fit(prepared).bestModel();

        return model;
    }

    public static void predictCrateData(SparkSession session, Properties properties, PipelineModel model) throws IOException, LangDetectException, URISyntaxException {
        // fetch data
        Dataset<Row> original = session
                .read()
                .jdbc(
                        properties.getProperty("connection-url"),
                        "(SELECT t.id, t.created_at, t.text from tweets t left join predicted_tweets p on t.id = p.id where p.id is null) as tweets",
                        properties
                );

        // ************
        // preparations
        // ************
        Dataset<Row> prepared = prepareTweets(original, 30, true);

        // predict crate data
        Dataset<Row> predicted = model.transform(prepared);

        // write data back to crate
        predicted.select("id", "created_at", TEXT_ORIGINAL, PREDICTION, LABEL_ORIGINAL)
                .write()
                .mode(SaveMode.Append)
                .jdbc(
                        properties.getProperty("connection-url"),
                        "predicted_tweets",
                        properties
                );
    }

}
