package io.connect.scylladb.utils;

//import jdk.nashorn.internal.ir.LiteralNode;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class ListRecommender implements ConfigDef.Recommender {
    private final List<Object> validValues;

    public ListRecommender(List<Object> validValues){
        this.validValues = validValues;
    }

    @Override
    public List<Object> validValues(String s, Map<String, Object> map) {
        return validValues;
    }

    @Override
    public boolean visible(String s, Map<String, Object> map) {
        return true;
    }
}
