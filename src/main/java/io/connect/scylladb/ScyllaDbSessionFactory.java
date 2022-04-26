package io.connect.scylladb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.extras.codecs.date.SimpleDateCodec;
import io.connect.scylladb.codec.ConvenienceCodecs;
import io.connect.scylladb.codec.StringDurationCodec;
import io.connect.scylladb.codec.StringInetCodec;
import io.connect.scylladb.codec.StringTimeUuidCodec;
import io.connect.scylladb.codec.StringUuidCodec;
import io.connect.scylladb.codec.StringVarintCodec;
import io.connect.scylladb.codec.StructUDTCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.json.JSONObject;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.List;

public class ScyllaDbSessionFactory {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSessionFactory.class);
  private static final CodecRegistry CODEC_REGISTRY = CodecRegistry.DEFAULT_INSTANCE;

  static {
    // Register custom codec once at class loading time; duplicates will be logged via warning
    CODEC_REGISTRY.register(StringUuidCodec.INSTANCE);
    CODEC_REGISTRY.register(StringTimeUuidCodec.INSTANCE);
    CODEC_REGISTRY.register(StringInetCodec.INSTANCE);
    CODEC_REGISTRY.register(StringVarintCodec.INSTANCE);
    CODEC_REGISTRY.register(StringDurationCodec.INSTANCE);
    CODEC_REGISTRY.register(SimpleDateCodec.instance);
    CODEC_REGISTRY.register(ConvenienceCodecs.ALL_INSTANCES);
  }

  public ScyllaDbSession newSession(ScyllaDbSinkConnectorConfig config) {
    Cluster.Builder clusterBuilder = Cluster.builder()
        .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
        .withCodecRegistry(CODEC_REGISTRY);

    try {
      configureAddressTranslator(config, clusterBuilder);
    } catch (JSONException e) {
      log.info("Failed to configure address translator, provide a valid JSON string " +
              "with external network address and port mapped to private network " +
              "address and port.");
      configurePublicContactPoints(config, clusterBuilder);
    }

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
        final KeyStore trustKeyStore = createKeyStore(config.trustStorePath, config.trustStorePassword);

        final TrustManagerFactory trustManagerFactory;
        try {
          trustManagerFactory =
              TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          trustManagerFactory.init(trustKeyStore);
        } catch (NoSuchAlgorithmException e) {
          throw new ConnectException("Exception while creating TrustManagerFactory", e);
        } catch (KeyStoreException e) {
          throw new ConnectException("Exception while calling TrustManagerFactory.init()", e);
        }
        sslContextBuilder.trustManager(trustManagerFactory);
      }

      if (null != config.keyStorePath) {
        log.info("Configuring SSLContext to use Keystore {}", config.keyStorePath);
        final KeyStore keyStore = createKeyStore(config.keyStorePath, config.keyStorePassword);

        final KeyManagerFactory keyManagerFactory;
        try {
          keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          keyManagerFactory.init(keyStore, config.keyStorePassword);
        } catch (NoSuchAlgorithmException e) {
          throw new ConnectException("Exception while creating KeyManagerFactory", e);
        } catch (UnrecoverableKeyException | KeyStoreException e) {
          throw new ConnectException("Exception while calling KeyManagerFactory.init()", e);
        }
        sslContextBuilder.keyManager(keyManagerFactory);
      }

      if (config.cipherSuites.size() > 0) {
        sslContextBuilder.ciphers(config.cipherSuites);
      }

      if (config.certFilePath != null && config.privateKeyPath != null) {
        try {
          sslContextBuilder.keyManager(new BufferedInputStream(new FileInputStream(config.certFilePath)),
                  new BufferedInputStream(new FileInputStream(config.privateKeyPath)));
        }
        catch (IllegalArgumentException e) {
          throw new ConnectException(String.format("Invalid certificate or private key: %s", e.getMessage()));
        } catch (FileNotFoundException e) {
          throw new ConnectException("Invalid certificate or private key file path", e);
        }
      } else if (config.certFilePath == null != (config.privateKeyPath == null)) {
        throw new ConnectException(String.format("%s cannot be set without %s and vice-versa: %s is not set",
                "scylladb.ssl.openssl.keyCertChain", "scylladb.ssl.openssl.privateKey",
                (config.certFilePath == null) ? "scylladb.ssl.openssl.keyCertChain" : "scylladb.ssl.openssl.privateKey"));
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
    final Session session = cluster.connect();
    KeyspaceMetadata ks = session.getCluster().getMetadata().getKeyspace(config.keyspace);
    if(ks != null) {
      Collection<UserType> userTypes = ks.getUserTypes();
      userTypes.stream().forEach(t -> {
        CODEC_REGISTRY.register(new StructUDTCodec(CODEC_REGISTRY, t));
      });
    }
    else{
      log.warn("Received null cluster metadata. Unable to register KafkaStruct to UserType codecs if any exist.");
    }

    return new ScyllaDbSessionImpl(config, cluster, session);
  }

  private void configurePublicContactPoints(ScyllaDbSinkConnectorConfig config, Cluster.Builder clusterBuilder) {
    log.info("Configuring public contact points={}", config.contactPoints);
    String[] contactPointsArray = config.contactPoints.split(",");
    clusterBuilder.withPort(config.port)
            .addContactPoints(contactPointsArray);
  }

  private void configureAddressTranslator(ScyllaDbSinkConnectorConfig config, Cluster.Builder clusterBuilder) {
    log.info("Trying to configure address translator for private network address and port.");
    new JSONObject(config.contactPoints);
    ClusterAddressTranslator translator = new ClusterAddressTranslator();
    translator.setMap(config.contactPoints);
    clusterBuilder.addContactPointsWithPorts(translator.getContactPoints())
            .withAddressTranslator(translator);
  }

  private KeyStore createKeyStore(File path, char[] password) {
    KeyStore keyStore;
    try {
      keyStore = KeyStore.getInstance("JKS");
      try (InputStream inputStream = new FileInputStream(path)) {
        keyStore.load(inputStream, password);
      } catch (IOException e) {
        throw new ConnectException("Exception while reading keystore", e);
      } catch (CertificateException | NoSuchAlgorithmException e) {
        throw new ConnectException("Exception while loading keystore", e);
      }
    } catch (KeyStoreException e) {
      throw new ConnectException("Exception while creating keystore", e);
    }
    return keyStore;
  }
}
