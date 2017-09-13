package crate.transformation;

import org.apache.spark.ml.param.Param;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;

import java.util.regex.Pattern;

/**
 * Transformer for String replacement using regular expressions.
 */
public class RegexReplacer extends IdentifiableUnaryTransformer<String, String, RegexReplacer> {

    private final Param<Pattern> pattern;
    private final Param<String> replacement;

    public RegexReplacer() {
        super();
        this.pattern = new Param<>(this, "pattern", "regex pattern used for matching");
        this.replacement = new Param<>(this, "replacement", "substitution text");
        this.setDefault(this.pattern(), Pattern.compile(""));
        this.setDefault(this.replacement(), "");
    }

    public Param<Pattern> pattern() {
        return this.pattern;
    }

    public RegexReplacer setPattern(String value) {
        return (RegexReplacer) this.set(this.pattern(), Pattern.compile(value));
    }

    public Pattern getPattern() {
        return this.$(this.pattern());
    }

    public Param<String> replacement() {
        return this.replacement;
    }

    public RegexReplacer setReplacement(String value) {
        return (RegexReplacer) this.set(this.replacement(), value);
    }

    public String getReplacement() {
        return this.$(this.replacement());
    }

    @Override
    public Function1<String, String> createTransformFunc() {
        return new SerializableAbstractFunction<String, String>() {
            @Override
            public String apply(String s) {
                return getPattern().matcher(s).replaceAll(getReplacement());
            }
        };
    }

    @Override
    public DataType outputDataType() {
        return DataTypes.StringType;
    }

    @Override
    public String getName() {
        return "regexreplacer";
    }
}
