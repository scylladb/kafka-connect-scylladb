package io.connect.scylladb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import io.connect.scylladb.codec.StringTimeUuidCodec;
import io.connect.scylladb.codec.StringUuidCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class ScyllaDbSessionFactory {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSessionFactory.class);
  private static final CodecRegistry CODEC_REGISTRY = CodecRegistry.DEFAULT_INSTANCE;

  static {
    // Register custom codec once at class loading time; duplicates will be logged via warning
    CODEC_REGISTRY.register(StringUuidCodec.INSTANCE);
    CODEC_REGISTRY.register(StringTimeUuidCodec.INSTANCE);
  }

  public ScyllaDbSession newSession(ScyllaDbSinkConnectorConfig config) {
    Cluster.Builder clusterBuilder = Cluster.builder()
        .withPort(config.port)
        .addContactPoints(config.contactPoints)
        .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
        .withCodecRegistry(CODEC_REGISTRY);
    if (!config.loadBalancingLocalDc.isEmpty()) {
      clusterBuilder.withLoadBalancingPolicy(
              DCAwareRoundRobinPolicy.builder()
                  .withLocalDc(config.loadBalancingLocalDc).build());
    } else {
      log.warn("`scylladb.loadbalancing.localdc` has not been configured, "
              + "which is recommended configuration in case of more than one DC.");
    }
    if (config.securityEnabled) {
      clusterBuilder.withCredentials(config.username, config.password);
    }
    if (config.sslEnabled) {
      final SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
      sslContextBuilder.sslProvider(config.sslProvider);

      if (null != config.trustStorePath) {
        log.info("Configuring SSLContext to use Truststore {}", config.trustStorePath);
        final KeyStore keyStore;
        try {
          keyStore = KeyStore.getInstance("JKS");
          try (InputStream inputStream = new FileInputStream(config.trustStorePath)) {
            keyStore.load(inputStream, config.trustStorePassword);
          } catch (IOException e) {
            throw new ConnectException("Exception while reading keystore", e);
          } catch (CertificateException | NoSuchAlgorithmException e) {
            throw new ConnectException("Exception while loading keystore", e);
          }
        } catch (KeyStoreException e) {
          throw new ConnectException("Exception while creating keystore", e);
        }

        final TrustManagerFactory trustManagerFactory;
        try {
          trustManagerFactory =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          trustManagerFactory.init(keyStore);
        } catch (NoSuchAlgorithmException e) {
          throw new ConnectException("Exception while creating TrustManagerFactory", e);
        } catch (KeyStoreException e) {
          throw new ConnectException("Exception while calling TrustManagerFactory.init()", e);
        }
        sslContextBuilder.trustManager(trustManagerFactory);
      }

      final SslContext context;
      try {
        context = sslContextBuilder.build();
      } catch (SSLException e) {
        throw new ConnectException(e);
      }
      final SSLOptions sslOptions = new RemoteEndpointAwareNettySSLOptions(context);
      clusterBuilder.withSSL(sslOptions);
    }
    clusterBuilder.withCompression(config.compression);
    Cluster cluster = clusterBuilder.build();
    log.info("Creating session");
    final Session session = cluster.newSession();
    return new ScyllaDbSessionImpl(config, cluster, session);
  }
}
