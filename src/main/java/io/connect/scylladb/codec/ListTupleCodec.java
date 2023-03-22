package io.connect.scylladb.codec;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.MappingCodec;
import com.google.common.reflect.TypeToken;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ListTupleCodec extends MappingCodec<List, TupleValue> {

    private CodecRegistry registry;
    private TupleType definition;

    public ListTupleCodec(CodecRegistry registry, TupleType definition) {
        super(TypeCodec.tuple(definition), List.class);
        this.registry = registry;
        this.definition = definition;
    }

    @Override
    protected TupleValue serialize(List list) {
        if (list == null) {
            return null;
        }

        int size = definition.getComponentTypes().size();
        int listSize = list.size();
        if (listSize != size) {
            throw new IllegalArgumentException(
                    String.format("Expecting %d fields, got %d", size, listSize));
        }

        TupleValue value = definition.newValue();
        Iterator iter = list.iterator();
        for(int i = 0; i < size; i++){
            Object item = iter.next();
            DataType elementType = definition.getComponentTypes().get(i);
            value.set(i, item, registry.codecFor(elementType, item));
        }
        return value;
    }

    @Override
    protected List deserialize(TupleValue value) { throw new UnsupportedOperationException(); }

}
