package io.connect.scylladb.codec;

import com.datastax.driver.core.*;
import com.datastax.driver.extras.codecs.MappingCodec;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Set;
import java.util.stream.Collectors;

public class StructTupleCodec extends MappingCodec<Struct, TupleValue> {

        private CodecRegistry registry;
        private TupleType definition;

        public StructTupleCodec(CodecRegistry registry, TupleType definition) {
            super(TypeCodec.tuple(definition), Struct.class);
            this.registry = registry;
            this.definition = definition;
        }

        @Override
        protected TupleValue serialize(Struct struct) {
            if (struct == null) {
                return null;
            }

            int size = definition.getComponentTypes().size();
            Schema schema = struct.schema();
            int structSize = schema.fields().size();
            Set<String> structFieldNames = schema.fields().stream().map(Field::name).collect(Collectors.toSet());
            if (structSize != size) {
                throw new IllegalArgumentException(
                        String.format("Expecting %d fields, got %d", size, structSize));
            }

            TupleValue value = definition.newValue();
            for(int i = 0; i < size; i++){
                Object field = struct.get(schema.fields().get(i));
                DataType elementType = definition.getComponentTypes().get(i);
                value.set(i, field, registry.codecFor(elementType, field));
            }
            return value;
        }

        @Override
        protected Struct deserialize(TupleValue value) { throw new UnsupportedOperationException(); }

}
