package crate.app;

import com.cybozu.labs.langdetect.LangDetectException;
import crate.util.Broadcaster;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import static crate.app.LearnFromData.learnFromTwitter;
import static crate.app.PredictCrateData.predictCrateData;
import static crate.meta.AppMetadata.MODEL_NAME;
import static crate.meta.AppMetadata.TABLE_NAME;
import static crate.meta.AppMetadata.parse;
import static crate.util.CrateBlobStorageUtil.save;

/**
 * This class simply combines LearnFromData and PredictCrateData.
 */
public class LearnAndPredict {

    public static void main(String[] args) throws URISyntaxException, IOException, LangDetectException, SQLException {
        // load properties
        Properties properties = parse(args);

        // initialize spark session
        SparkSession session = SparkSession
                .builder()
                .appName("LearnAndPredictLanguage")
                .getOrCreate();

        PipelineModel model = learnFromTwitter(session, properties);
        predictCrateData(session, properties, model);

        // broadcast model so it's completely available on every node
        Broadcast<PipelineModel> modelBroadcast = Broadcaster.broadcast(session, model);
        save(properties, TABLE_NAME, MODEL_NAME, modelBroadcast.getValue());

        session.stop();
    }
}
