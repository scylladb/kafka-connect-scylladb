package io.connect.scylladb.utils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class ScyllaDbConstants {

    public static final String DELETE_OPERATION = "delete";

    public static final String INSERT_OPERATION = "insert";

    // Not a valid unquoted CQL name, so it is quoted, and cannot clash with an unquoted column name.
    public static final CqlIdentifier TTL_BIND_MARKER = CqlIdentifier.fromInternal("__ttl");

}
