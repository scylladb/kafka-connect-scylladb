package io.connect.scylladb;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
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
    /** The cluster is unreachable, slow or overloaded; retrying may succeed. */
    TRANSIENT,
    /** The connector is misconfigured or failed unexpectedly; no record can be written. */
    FATAL
  }

  // Prefix of ScyllaDB's rate_limit_exception message (exceptions/exceptions.cc).
  private static final String RATE_LIMIT_MESSAGE = "Per-partition rate limit reached";

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
            || isAuthFailure(cause)) {
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
   * and tables created per table, so a rejected query there is not the record's fault.
   */
  static Kind classifyConversion(Throwable error) {
    Throwable cause = unwrap(error);
    if ((cause instanceof DataException && !(cause instanceof TableConfigurationException))
            || cause instanceof NullPointerException
            || cause instanceof CodecNotFoundException) {
      return Kind.RECORD;
    }
    Kind kind = classify(cause);
    return kind == Kind.RECORD ? Kind.FATAL : kind;
  }

  // Without the RATE_LIMIT_ERROR protocol extension, which the driver does not negotiate, ScyllaDB
  // sends a per-partition rate-limit rejection as CONFIG_ERROR; only the message tells them apart.
  private static boolean isRateLimited(Throwable error) {
    return error instanceof InvalidConfigurationInQueryException
            && error.getMessage() != null
            && error.getMessage().startsWith(RATE_LIMIT_MESSAGE);
  }

  // The driver reports rejected credentials at connect time as every node failing to authenticate.
  private static boolean isAuthFailure(Throwable error) {
    if (!(error instanceof AllNodesFailedException)) {
      return false;
    }
    boolean any = false;
    for (List<Throwable> nodeErrors : ((AllNodesFailedException) error).getAllErrors().values()) {
      for (Throwable nodeError : nodeErrors) {
        if (!(nodeError instanceof AuthenticationException)) {
          return false;
        }
        any = true;
      }
    }
    return any;
  }
}
