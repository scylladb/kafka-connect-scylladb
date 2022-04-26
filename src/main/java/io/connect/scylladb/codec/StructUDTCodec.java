package io.connect.scylladb.codec;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.extras.codecs.MappingCodec;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Set;
import java.util.stream.Collectors;

public class StructUDTCodec extends MappingCodec<Struct, UDTValue> {

    CodecRegistry registry;
    UserType definition;

    public StructUDTCodec(CodecRegistry registry, UserType udt) {
        super(registry.codecFor(udt), Struct.class);
        this.registry = registry;
        this.definition = udt;
    }

    @Override
    protected UDTValue serialize(Struct struct) {
        if (struct == null) {
            return null;
        }

        int size = definition.getFieldNames().size();
        Schema schema = struct.schema();
        int structSize = schema.fields().size();
        Set<String> structFieldNames = schema.fields().stream().map(Field::name).collect(Collectors.toSet());
        if (structSize != size) {
            throw new IllegalArgumentException(
                    String.format("Expecting %d fields, got %d", size, structSize));
        }

        final UDTValue value = definition.newValue();
        definition.getFieldNames().stream().forEach(fieldName -> {
            if (!structFieldNames.contains(fieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Field %s in UDT %s not found in input struct",
                                fieldName, definition.getName()));
            }
            DataType fieldType = definition.getFieldType(fieldName);
            value.set(fieldName, struct.get(fieldName), registry.codecFor(fieldType, struct.get(fieldName)));
            }
        );

        return value;
    }

    @Override
    protected Struct deserialize(UDTValue value) { throw new UnsupportedOperationException(); }
}
