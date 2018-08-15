package base.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public enum SJConfig {

    projectLocDirConfig("proj.config.location.dir"), // NOSONAR
    projectLocOutdirConfig("proj.config.location.outdir"), // NOSONAR
    sparkSessionPropMaster("spark.session.prop.master"), // NOSONAR
    sparkSessionPropAppname("spark.session.prop.appname"); // NOSONAR

    private final String propertyKey;

    private String value;

    static {
        Properties prop = new Properties();
        try (//
            InputStream input = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("config.properties");) {//
                prop.load(input);//
        } catch (IOException | IllegalArgumentException | NullPointerException exception) {
            throw new RuntimeException("Could not load property file.", exception);// NOSONAR
        }

        for (SJConfig message : SJConfig.values()) {//
            String value = prop.getProperty(message.propertyKey);//
            if (value == null) {//
                throw new RuntimeException("Property <" + message.propertyKey + "> not found in properties file!"); // NOSONAR
            }
            message.value = value;
        }
    }

    private SJConfig(String propertyKey) {
        this.propertyKey = propertyKey;
    }

    @Override
    public String toString() {
        return value;
    }

    public String getValue() {
        return value;
    }

    public String format(Object... arguments) {
        return String.format(value, arguments);
    }
}

