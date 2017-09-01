package crate.util;

import com.cybozu.labs.langdetect.LangDetectException;
import crate.transformation.LanguageGuesser;
import crate.transformation.RegexReplacer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

import static crate.meta.Metadata.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;

public class TwitterUtil {

    private static Properties properties;

    public static final Properties properties() throws IOException {
        if(properties==null) {
            properties = new Properties();
            properties.load(TwitterUtil.class.getResourceAsStream("/crate.properties"));

        }
        return properties;
    }

    public static final Dataset<Row> prepareTweets(Dataset<Row> original, int tweetMinLength, boolean label) throws LangDetectException {
        final String transformationPattern = "(&\\w+;)"
                //retweets
                + "|(^RT @\\w+: )"
                //emails
                + "|(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])" //e-mails
                //user-tags
                + "|(@\\w+)"
                //links
                + "|((https?|ftp):\\/\\/[^\\s/$.?#].[^\\s]*)"
                //hashtags
                + "|(#\\w+)"
                //emojis
                + "|([\\u203C-\\uDFFF])";

        // remove retweets, email-addresses, user-mentionings, links, hashtags, emojis
        RegexReplacer regexReplacer = new RegexReplacer()
                .setInputCol(TEXT_ORIGINAL)
                .setOutputCol(TEXT_FILTERED)
                .setPattern(transformationPattern)
                .setReplacement("");
        Dataset<Row> rawFiltered = regexReplacer.transform(original);

        // remove tweets which are shorter than minimum length
        Dataset<Row> filtered = rawFiltered.filter(length(col(TEXT_FILTERED)).geq(tweetMinLength));

        if (label) {
            // label tweets
            LanguageGuesser languageGuesser = new LanguageGuesser()
                    .setInputCol(TEXT_FILTERED)
                    .setOutputCol(LABEL_ORIGINAL);
            return languageGuesser.transform(filtered);
        }

        return filtered;
    }
}
