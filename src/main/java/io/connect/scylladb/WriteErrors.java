package io.connect.scylladb;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import org.apache.kafka.connect.errors.DataException;

/**
 * Classifies failures raised while converting or writing a record.
 */
final class WriteErrors {

  enum Kind {
    /** The record itself is at fault; retrying cannot succeed. */
    RECORD,
    /** The cluster is unreachable, slow, overloaded or failed the write on a replica; retrying may succeed. */
    TRANSIENT,
    /** The connector or its tables are misconfigured, or ScyllaDB or the connector failed unexpectedly. */
    FATAL
  }

  // Prefix of ScyllaDB's rate_limit_exception message (exceptions/exceptions.cc).
  private static final String RATE_LIMIT_MESSAGE = "Per-partition rate limit reached";
  // ScyllaDB's messages for a statement on a table, column or keyspace that does not exist.
  private static final String MISSING_TABLE_MESSAGE = "unconfigured table";
  private static final String MISSING_COLUMN_MESSAGE = "Unknown identifier ";
  private static final Pattern MISSING_KEYSPACE_MESSAGE = Pattern.compile("Keyspace .+ does not exist");
  // ScyllaDB's message for a write consistency level its write_consistency_levels_disallowed guardrail forbids.
  private static final String DISALLOWED_CONSISTENCY_MESSAGE = "Write consistency level ";
  private static final String DISALLOWED_CONSISTENCY_SETTING = "write_consistency_levels_disallowed";

  private WriteErrors() {
  }

  static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while ((current instanceof ExecutionException || current instanceof CompletionException)
            && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  /**
   * Classifies the failure of an attempt to write a record, whether the driver raised it before
   * sending the write (such as a frame too long to encode) or ScyllaDB answered with an error.
   */
  static Kind classify(Throwable error) {
    Throwable cause = unwrap(error);
    if (isRateLimited(cause)) {
      return Kind.TRANSIENT;
    }
    if (cause instanceof TableConfigurationException
            || cause instanceof UnauthorizedException
            || cause instanceof AuthenticationException
            || cause instanceof SyntaxError
            || cause instanceof InvalidConfigurationInQueryException
            || cause instanceof ServerError
            || cause instanceof ProtocolError
            || isMissingSchema(cause)
            || isDisallowedConsistency(cause)
            || failedFatallyOnEveryNode(cause)) {
      return Kind.FATAL;
    }
    if (cause instanceof InvalidQueryException
            || cause instanceof FunctionFailureException
            || cause instanceof CodecNotFoundException
            || cause instanceof FrameTooLongException
            || cause instanceof DataException
            || cause instanceof NullPointerException) {
      return Kind.RECORD;
    }
    // A concurrent CREATE TABLE won the race; the table exists on the next attempt.
    if (cause instanceof AlreadyExistsException) {
      return Kind.TRANSIENT;
    }
    if (cause instanceof DriverException || cause instanceof TimeoutException) {
      return Kind.TRANSIENT;
    }
    return Kind.FATAL;
  }

  /**
   * Classifies a failure raised while turning a record into a statement. Statements are prepared
   * and tables created per table, so a rejected query there is not the record's fault: only the
   * record errors the conversion itself raises stay {@link Kind#RECORD}.
   */
  static Kind classifyConversion(Throwable error) {
    Throwable cause = unwrap(error);
    Kind kind = classify(cause);
    if (kind == Kind.RECORD
            && !(cause instanceof DataException
                || cause instanceof NullPointerException
                || cause instanceof CodecNotFoundException)) {
      return Kind.FATAL;
    }
    return kind;
  }

  // Without the RATE_LIMIT_ERROR protocol extension, which the driver does not negotiate, ScyllaDB
  // sends a per-partition rate-limit rejection as CONFIG_ERROR; only the message tells them apart.
  private static boolean isRateLimited(Throwable error) {
    return error instanceof InvalidConfigurationInQueryException
            && error.getMessage() != null
            && error.getMessage().startsWith(RATE_LIMIT_MESSAGE);
  }

  // A keyspace, table or column dropped while the task runs fails each write with INVALID, as a record
  // ScyllaDB rejects does; only the message tells them apart.
  private static boolean isMissingSchema(Throwable error) {
    if (!(error instanceof InvalidQueryException) || error.getMessage() == null) {
      return false;
    }
    String message = error.getMessage();
    return message.startsWith(MISSING_TABLE_MESSAGE)
            || message.startsWith(MISSING_COLUMN_MESSAGE)
            || MISSING_KEYSPACE_MESSAGE.matcher(message).matches();
  }

  // A cluster that forbids the configured write consistency level rejects every write with INVALID; only the
  // message tells it apart from a record ScyllaDB rejects.
  private static boolean isDisallowedConsistency(Throwable error) {
    return error instanceof InvalidQueryException
            && error.getMessage() != null
            && error.getMessage().startsWith(DISALLOWED_CONSISTENCY_MESSAGE)
            && error.getMessage().contains(DISALLOWED_CONSISTENCY_SETTING);
  }

  // The driver reports a failure on every node it tried as one exception, such as rejected credentials at
  // connect time; it is fatal when every node failed fatally.
  private static boolean failedFatallyOnEveryNode(Throwable error) {
    if (!(error instanceof AllNodesFailedException)) {
      return false;
    }
    boolean any = false;
    for (List<Throwable> nodeErrors : ((AllNodesFailedException) error).getAllErrors().values()) {
      for (Throwable nodeError : nodeErrors) {
        if (classify(nodeError) != Kind.FATAL) {
          return false;
        }
        any = true;
      }
    }
    return any;
  }
}
