package io.connect.scylladb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class VersionUtil {
    private static final Logger log = LoggerFactory.getLogger(VersionUtil.class);
    private static final Properties VERSION = loadVersion();

    private static Properties loadVersion() {
        // Load the version from version.properties resource file.
        InputStream inputStream = VersionUtil.class.getClassLoader().getResourceAsStream("io/connect/scylladb/version.properties");
        Properties props = new Properties();
        try {
            props.load(inputStream);
        } catch (IOException e) {
            log.error("Error loading the connector version");
        }
        return props;
    }

    public static String getVersion() {
        return VERSION.getProperty("version");
    }
}
