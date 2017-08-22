package crate.transformation;

import com.sun.istack.internal.NotNull;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.regex.Pattern;

public class RegexFunction extends AbstractFunction1<String, String> implements Serializable {

    private final Pattern pattern;
    private final String replacement;

    public RegexFunction(@NotNull String pattern, @NotNull String replacement) {
        this.pattern = Pattern.compile(pattern);
        this.replacement = replacement;
    }

    @Override
    public String apply(String s) {
        return pattern.matcher(s).replaceAll(replacement);
    }
}
