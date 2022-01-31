package io.connect.scylladb.utils;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VisibleIfEqual implements ConfigDef.Recommender {

    private final String configName;
    private final Object expectedValue;
    private final List<Object> validValues;


    public VisibleIfEqual(String configName, Object expectedValue){
        this(configName, expectedValue, Collections.emptyList());
    }

    public VisibleIfEqual(String configName, Object expectedValue, List<Object> validValues){
        this.configName = configName;
        this.expectedValue = expectedValue;
        this.validValues = validValues;
    }

    @Override
    public List<Object> validValues(String s, Map<String, Object> map) {
        return validValues;
    }

    @Override
    public boolean visible(String s, Map<String, Object> map) {
        Object value = map.get(configName);
        if(value == null) {
            return false;
        }
        return value.equals(expectedValue);
    }
}
