package crate.util;

import com.google.common.base.Joiner;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ArgumentParser {

    public static Properties parse(String[] args, OptionParser parser, String prefix) {
        return asProperties(parser.parse(args), prefix);
    }

    private static Properties asProperties(OptionSet options, String prefix) {
        Properties properties = new Properties();
        for (Map.Entry<OptionSpec<?>, List<?>> entry : options.asMap().entrySet()) {
            OptionSpec<?> spec = entry.getKey();
            properties.setProperty(
                    asPropertyKey(prefix, spec),
                    asPropertyValue(entry.getValue(), options.has(spec)));
        }
        return properties;
    }

    private static String asPropertyKey(String prefix, OptionSpec<?> spec) {
        List<String> flags = spec.options();
        for (String flag : flags)
            if (1 < flag.length())
                return null == prefix ? flag : (prefix + '.' + flag);
        throw new IllegalArgumentException("No usable non-short flag: " + flags);
    }

    private static String asPropertyValue(List<?> values, boolean present) {
        // Simple flags have no values; treat presence/absence as true/false
        return values.isEmpty() ? String.valueOf(present) : Joiner.on(",").join(values);
    }
}
