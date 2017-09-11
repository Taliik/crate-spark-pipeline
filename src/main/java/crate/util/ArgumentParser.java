package crate.util;

import com.google.common.base.Joiner;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static crate.meta.Metadata.*;
import static crate.meta.Metadata.CRATE_HTTP_PORT;
import static crate.meta.Metadata.TABLE_NAME;

/**
 * Used to parse main arguments and feed them into a Properties-object.
 */
public class ArgumentParser {

    public static Properties parse(String[] args) {
        OptionParser parser = new OptionParser();
        parser.acceptsAll(Arrays.asList("c", CRATE_HOST), "crate-host to connect to e.g. localhost").withRequiredArg().defaultsTo("localhost");
        parser.acceptsAll(Arrays.asList("h", CRATE_HTTP_PORT), "http-port used to connect to crate via http e.g. 4200").withRequiredArg().defaultsTo("4200");
        parser.acceptsAll(Arrays.asList("p", CRATE_PSQL_PORT), "psql-port used to connect to crate via psql e.g. 5432").withRequiredArg().defaultsTo("5432");
        parser.acceptsAll(Arrays.asList("u", CRATE_USER), "crate user for connection e.g. crate").withRequiredArg().defaultsTo("crate");
        parser.acceptsAll(Arrays.asList("d", CRATE_DRIVER), "crate jdbc driver class e.g. io.crate.client.jdbc.CrateDriver").withRequiredArg().defaultsTo("io.crate.client.jdbc.CrateDriver");

        Properties properties = parse(args, parser, null);
        properties.setProperty(CRATE_JDBC_CONNECTION_URL, String.format("jdbc:crate://%s:%s/?strict=true", properties.getProperty(CRATE_HOST), properties.getProperty(CRATE_PSQL_PORT)));
        properties.setProperty(CRATE_BLOB_CONNECTION_URL, String.format("http://%s:%s/_blobs/%s/", properties.getProperty(CRATE_HOST), properties.getProperty(CRATE_HTTP_PORT), TABLE_NAME));

        return properties;
    }

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
