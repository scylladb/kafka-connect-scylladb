package io.connect.scylladb.utils;

import org.apache.kafka.common.config.ConfigDef;

import java.io.File;

public class NullOrReadableFile implements ConfigDef.Validator {
    @Override
    public void ensureValid(String s, Object o) {
        if(o != null){
            File file = new File(o.toString());
            if(!file.canRead()){
                throw new IllegalArgumentException("Cannot read file '" + o + "' provided through '" + s + "'.");
            }
        }
    }
}
