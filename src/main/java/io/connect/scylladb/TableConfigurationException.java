package io.connect.scylladb;

import org.apache.kafka.connect.errors.DataException;

/**
 * The target table is missing or does not match the connector configuration, so no record for it
 * can be written. Unlike other {@link DataException}s, it is not the record's fault.
 */
final class TableConfigurationException extends DataException {

  TableConfigurationException(String message) {
    super(message);
  }
}
