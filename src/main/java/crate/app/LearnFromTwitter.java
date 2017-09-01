package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
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
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static crate.meta.Metadata.*;
import static crate.util.TwitterUtil.*;

public class LearnFromTwitter {

    public static void main(String[] args) throws IOException, LangDetectException {


        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("Learn From Twitter")
                .master(spark.getProperty("spark.master"))
                .getOrCreate();


        learnFromTwitter(session);

        session.stop();
    }

    public static void learnFromTwitter(SparkSession session) throws LangDetectException, IOException {
        // fetch data
        Dataset<Row> original = session
                .read()
                .jdbc(
                        crate.getProperty("url"),
                        "(SELECT text from tweets) as tweets",
                        crate
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

        // Save the model and delete old one if existing
        String modelFileName = spark.getProperty("spark.model");
        if (Files.isDirectory(Paths.get(modelFileName))) {
            Path rootPath = Paths.get(modelFileName);
            Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        model.save(modelFileName);

    }

}
