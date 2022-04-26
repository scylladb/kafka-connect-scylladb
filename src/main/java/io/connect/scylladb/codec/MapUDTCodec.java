package io.connect.scylladb.codec;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.extras.codecs.MappingCodec;
import com.google.common.reflect.TypeToken;

import java.util.Map;

public class MapUDTCodec extends MappingCodec<Map, UDTValue> {
    CodecRegistry registry;
    UserType definition;

    public MapUDTCodec(CodecRegistry registry, UserType udt) {
        super(registry.codecFor(udt), Map.class);
        this.registry = registry;
        this.definition = udt;
    }

    @Override
    protected UDTValue serialize(Map map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        if(!(map.keySet().iterator().next() instanceof String)){
            throw new UnsupportedOperationException("This codec (" + this.getClass().getSimpleName()
                + ") handles only Maps that have String as their key type.");
        }
        int size = definition.getFieldNames().size();
        int mapSize = map.size();
        if (mapSize != size) {
            throw new IllegalArgumentException(
                    String.format("Expecting %d fields, got %d", size, mapSize));
        }

        final UDTValue value = definition.newValue();
        definition.getFieldNames().stream().forEach(fieldName -> {
                    if (!map.containsKey(fieldName)) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Field %s in UDT %s not found in input map",
                                        fieldName, definition.getName()));
                    }
                    DataType fieldType = definition.getFieldType(fieldName);
                    value.set(fieldName, map.get(fieldName), registry.codecFor(fieldType, map.get(fieldName)));
                }
        );

        return value;
    }

    @Override
    protected Map deserialize(UDTValue value) {
        throw new UnsupportedOperationException("This codec (" + this.getClass().getSimpleName() + ") does not support deserialization from UDT to Map");
    }
}
