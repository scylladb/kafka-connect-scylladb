package io.connect.scylladb.integration;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.kafka.connect.data.*;

import java.util.*;

public class StructUtil {
    private StructUtil() {

    }

    public static SchemaAndValue asSchemaAndValue(Struct struct) {
        Preconditions.checkNotNull(struct, "struct cannot be null.");
        return new SchemaAndValue(struct.schema(), struct);
    }

    public static Map<String, Object> asMap(Struct struct) {
        Preconditions.checkNotNull(struct, "struct cannot be null.");
        Map<String, Object> result = new LinkedHashMap<>(struct.schema().fields().size());

        for (Field field : struct.schema().fields()) {
            final Object value;
            if (Schema.Type.STRUCT == field.schema().type()) {
                Struct s = struct.getStruct(field.name());
                value = asMap(s);
            } else {
                value = struct.get(field);
            }
            result.put(field.name(), value);
        }

        return result;
    }


    static class FieldState {
        final String name;
        final Schema.Type type;
        final boolean isOptional;
        final Object value;

        private FieldState(String name, Schema.Type type, boolean isOptional, Object value) {
            this.name = name;
            this.type = type;
            this.isOptional = isOptional;
            this.value = value;
        }

        static FieldState of(String name, Schema.Type type, boolean isOptional, Object value) {
            return new FieldState(name, type, isOptional, value);
        }

    }

    private static Struct struct(String name, List<FieldState> fields) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        if (!Strings.isNullOrEmpty(name)) {
            builder.name(name);
        }

        for (FieldState field : fields) {
            final Schema schema;
            if (Schema.Type.STRUCT == field.type) {
                Preconditions.checkNotNull(
                        field.value,
                        "%s field.value cannot be null. Struct is needed to infer schema of nested struct."
                );
                Struct struct = (Struct) field.value;
                schema = struct.schema();
            } else {
                SchemaBuilder fieldBuilder = SchemaBuilder.type(field.type);
                if (field.isOptional) {
                    fieldBuilder.optional();
                }
                schema = fieldBuilder.build();
            }
            builder.field(field.name, schema);
        }

        final Schema schema = builder.build();
        final Struct struct = new Struct(schema);

        for (FieldState field : fields) {
            struct.put(field.name, field.value);
        }

        struct.validate();
        return struct;
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1
    ) {
        return struct(
                name,
                Collections.singletonList(
                        FieldState.of(f1, t1, o1, v1)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3,
            String f4,
            Schema.Type t4,
            boolean o4,
            Object v4
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3),
                        FieldState.of(f4, t4, o4, v4)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3,
            String f4,
            Schema.Type t4,
            boolean o4,
            Object v4,
            String f5,
            Schema.Type t5,
            boolean o5,
            Object v5
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3),
                        FieldState.of(f4, t4, o4, v4),
                        FieldState.of(f5, t5, o5, v5)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3,
            String f4,
            Schema.Type t4,
            boolean o4,
            Object v4,
            String f5,
            Schema.Type t5,
            boolean o5,
            Object v5,
            String f6,
            Schema.Type t6,
            boolean o6,
            Object v6
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3),
                        FieldState.of(f4, t4, o4, v4),
                        FieldState.of(f5, t5, o5, v5),
                        FieldState.of(f6, t6, o6, v6)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3,
            String f4,
            Schema.Type t4,
            boolean o4,
            Object v4,
            String f5,
            Schema.Type t5,
            boolean o5,
            Object v5,
            String f6,
            Schema.Type t6,
            boolean o6,
            Object v6,
            String f7,
            Schema.Type t7,
            boolean o7,
            Object v7
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3),
                        FieldState.of(f4, t4, o4, v4),
                        FieldState.of(f5, t5, o5, v5),
                        FieldState.of(f6, t6, o6, v6),
                        FieldState.of(f7, t7, o7, v7)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3,
            String f4,
            Schema.Type t4,
            boolean o4,
            Object v4,
            String f5,
            Schema.Type t5,
            boolean o5,
            Object v5,
            String f6,
            Schema.Type t6,
            boolean o6,
            Object v6,
            String f7,
            Schema.Type t7,
            boolean o7,
            Object v7,
            String f8,
            Schema.Type t8,
            boolean o8,
            Object v8
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3),
                        FieldState.of(f4, t4, o4, v4),
                        FieldState.of(f5, t5, o5, v5),
                        FieldState.of(f6, t6, o6, v6),
                        FieldState.of(f7, t7, o7, v7),
                        FieldState.of(f8, t8, o8, v8)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3,
            String f4,
            Schema.Type t4,
            boolean o4,
            Object v4,
            String f5,
            Schema.Type t5,
            boolean o5,
            Object v5,
            String f6,
            Schema.Type t6,
            boolean o6,
            Object v6,
            String f7,
            Schema.Type t7,
            boolean o7,
            Object v7,
            String f8,
            Schema.Type t8,
            boolean o8,
            Object v8,
            String f9,
            Schema.Type t9,
            boolean o9,
            Object v9
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3),
                        FieldState.of(f4, t4, o4, v4),
                        FieldState.of(f5, t5, o5, v5),
                        FieldState.of(f6, t6, o6, v6),
                        FieldState.of(f7, t7, o7, v7),
                        FieldState.of(f8, t8, o8, v8),
                        FieldState.of(f9, t9, o9, v9)
                )
        );
    }

    public static Struct struct(
            String name,
            String f1,
            Schema.Type t1,
            boolean o1,
            Object v1,
            String f2,
            Schema.Type t2,
            boolean o2,
            Object v2,
            String f3,
            Schema.Type t3,
            boolean o3,
            Object v3,
            String f4,
            Schema.Type t4,
            boolean o4,
            Object v4,
            String f5,
            Schema.Type t5,
            boolean o5,
            Object v5,
            String f6,
            Schema.Type t6,
            boolean o6,
            Object v6,
            String f7,
            Schema.Type t7,
            boolean o7,
            Object v7,
            String f8,
            Schema.Type t8,
            boolean o8,
            Object v8,
            String f9,
            Schema.Type t9,
            boolean o9,
            Object v9,
            String f10,
            Schema.Type t10,
            boolean o10,
            Object v10
    ) {
        return struct(
                name,
                Arrays.asList(
                        FieldState.of(f1, t1, o1, v1),
                        FieldState.of(f2, t2, o2, v2),
                        FieldState.of(f3, t3, o3, v3),
                        FieldState.of(f4, t4, o4, v4),
                        FieldState.of(f5, t5, o5, v5),
                        FieldState.of(f6, t6, o6, v6),
                        FieldState.of(f7, t7, o7, v7),
                        FieldState.of(f8, t8, o8, v8),
                        FieldState.of(f9, t9, o9, v9),
                        FieldState.of(f10, t10, o10, v10)
                )
        );
    }
}
