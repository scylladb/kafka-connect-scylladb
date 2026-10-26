package io.connect.scylladb;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class WriteErrorsTest {

  private final Node node = mock(Node.class);

  private AuthenticationException authFailure() {
    return new AuthenticationException(mock(EndPoint.class), "denied");
  }

  @Test
  public void recordErrors() {
    assertEquals(WriteErrors.Kind.RECORD, WriteErrors.classify(new InvalidQueryException(node, "Key may not be empty")));
    assertEquals(WriteErrors.Kind.RECORD, WriteErrors.classify(new FunctionFailureException(node, "failed")));
    assertEquals(WriteErrors.Kind.RECORD,
        WriteErrors.classify(new CodecNotFoundException(DataTypes.INT, GenericType.STRING)));
    assertEquals(WriteErrors.Kind.RECORD,
        WriteErrors.classify(new FrameTooLongException(new InetSocketAddress(9042), "too long")));
    assertEquals(WriteErrors.Kind.RECORD, WriteErrors.classify(new DataException("bad record")));
    assertEquals(WriteErrors.Kind.RECORD, WriteErrors.classify(new NullPointerException()));
  }

  @Test
  public void transientErrors() {
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classify(new NoNodeAvailableException()));
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classify(new DriverTimeoutException("timeout")));
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classify(new OverloadedException(node)));
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classify(
        new WriteTimeoutException(node, DefaultConsistencyLevel.ONE, 0, 1, DefaultWriteType.SIMPLE)));
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classify(new TimeoutException()));
    assertEquals(WriteErrors.Kind.TRANSIENT,
        WriteErrors.classify(new AlreadyExistsException(node, "ks", "table")));
  }

  @Test
  public void fatalErrors() {
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(new UnauthorizedException(node, "denied")));
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(authFailure()));
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(new SyntaxError(node, "bad query")));
    assertEquals(WriteErrors.Kind.FATAL,
        WriteErrors.classify(new InvalidConfigurationInQueryException(node, "bad options")));
  }

  @Test
  public void tableConfigurationErrorsAreFatal() {
    TableConfigurationException missingTable = new TableConfigurationException("Table 'ks.t' does not exist.");
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(missingTable));
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classifyConversion(missingTable));
  }

  @Test
  public void rateLimitedWritesAreTransient() {
    // ScyllaDB's message when the client lacks the RATE_LIMIT_ERROR protocol extension.
    InvalidConfigurationInQueryException rateLimited = new InvalidConfigurationInQueryException(node,
        "Per-partition rate limit reached for write in table ks.table, rejected by coordinator");
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classify(rateLimited));
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classifyConversion(rateLimited));
  }

  @Test
  public void unexpectedErrorsAreFatal() {
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(new NoSuchElementException()));
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(new IllegalStateException()));
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(new ConnectException("Keyspace not found")));
  }

  @Test
  public void rejectedCredentialsOnEveryNodeAreFatal() {
    Map<Node, Throwable> errors = new LinkedHashMap<>();
    errors.put(node, authFailure());
    errors.put(mock(Node.class), authFailure());
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classify(AllNodesFailedException.fromErrors(errors)));

    errors.put(mock(Node.class), new DriverTimeoutException("timeout"));
    assertEquals(WriteErrors.Kind.TRANSIENT, WriteErrors.classify(AllNodesFailedException.fromErrors(errors)));
    assertEquals(WriteErrors.Kind.TRANSIENT,
        WriteErrors.classify(AllNodesFailedException.fromErrors(Collections.<Node, Throwable>emptyMap())));
  }

  @Test
  public void rejectedStatementDuringConversionIsFatal() {
    assertEquals(WriteErrors.Kind.FATAL,
        WriteErrors.classifyConversion(new InvalidQueryException(node, "unconfigured table")));
    assertEquals(WriteErrors.Kind.TRANSIENT,
        WriteErrors.classifyConversion(new AlreadyExistsException(node, "ks", "table")));
    assertEquals(WriteErrors.Kind.RECORD, WriteErrors.classifyConversion(new DataException("bad record")));
    assertEquals(WriteErrors.Kind.RECORD,
        WriteErrors.classifyConversion(new CodecNotFoundException(DataTypes.INT, GenericType.STRING)));
    assertEquals(WriteErrors.Kind.FATAL, WriteErrors.classifyConversion(new ConnectException("Keyspace not found")));
  }

  @Test
  public void unwrapsFutureExceptions() {
    InvalidQueryException cause = new InvalidQueryException(node, "Key may not be empty");
    Throwable wrapped = new ExecutionException(new CompletionException(cause));
    assertSame(cause, WriteErrors.unwrap(wrapped));
    assertEquals(WriteErrors.Kind.RECORD, WriteErrors.classify(wrapped));
    assertEquals(WriteErrors.Kind.TRANSIENT,
        WriteErrors.classify(new ExecutionException(new NoNodeAvailableException())));
  }
}
