package io.connect.scylladb;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import io.connect.scylladb.codec.ConvenienceCodecs;
import io.connect.scylladb.codec.StringDurationCodec;
import io.connect.scylladb.codec.StringInetCodec;
import io.connect.scylladb.codec.StringTimeUuidCodec;
import io.connect.scylladb.codec.StringUuidCodec;
import io.connect.scylladb.codec.StringVarintCodec;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class ScyllaDbSessionFactory {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSessionFactory.class);
  private static final MutableCodecRegistry CODEC_REGISTRY = new DefaultCodecRegistry("ScyllaDbSessionFactory.CodecRegistry");

  static {
    // Register custom codec once at class loading time; duplicates will be logged via warning
    CODEC_REGISTRY.register(StringUuidCodec.INSTANCE);
    CODEC_REGISTRY.register(StringTimeUuidCodec.INSTANCE);
    CODEC_REGISTRY.register(StringInetCodec.INSTANCE);
    CODEC_REGISTRY.register(StringVarintCodec.INSTANCE);
    CODEC_REGISTRY.register(StringDurationCodec.INSTANCE);
    CODEC_REGISTRY.register(ConvenienceCodecs.ALL_INSTANCES);
  }

  public ScyllaDbSession newSession(ScyllaDbSinkConnectorConfig config) {

    ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
    CqlSessionBuilder sessionBuilder = CqlSession.builder().withCodecRegistry(CODEC_REGISTRY);
    try {
      configureAddressTranslator(config, sessionBuilder, driverConfigLoaderBuilder);
    } catch (JsonProcessingException e) {
      log.info("Failed to configure address translator, provide a valid JSON string " +
              "with external network address and port mapped to private network " +
              "address and port.");
      configurePublicContactPoints(config, sessionBuilder);
    }

    driverConfigLoaderBuilder.withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DcInferringLoadBalancingPolicy.class);
    if (!config.loadBalancingLocalDc.isEmpty()) {
      sessionBuilder.withLocalDatacenter(config.loadBalancingLocalDc);
    } else {
      log.warn("`scylladb.loadbalancing.localdc` has not been configured, "
              + "which is recommended configuration in case of more than one DC.");
    }
    if (config.securityEnabled) {
      sessionBuilder.withAuthCredentials(config.username, config.password);
    }

    if (config.sslEnabled) {
      driverConfigLoaderBuilder
          .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
          .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, config.sslHostnameVerificationEnabled);

      if (null != config.trustStorePath) {
        log.info("Configuring Driver ({}) to use Truststore {}", DefaultDriverOption.SSL_TRUSTSTORE_PATH.getPath(), config.trustStorePath);
        driverConfigLoaderBuilder
            .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, config.trustStorePath.getAbsolutePath())
            .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, String.valueOf(config.trustStorePassword));
      }

      if (null != config.keyStorePath) {
        log.info("Configuring Driver ({}) to use Keystore {}", DefaultDriverOption.SSL_KEYSTORE_PATH.getPath(), config.keyStorePath);
        driverConfigLoaderBuilder
            .withString(DefaultDriverOption.SSL_KEYSTORE_PATH, config.keyStorePath.getAbsolutePath())
            .withString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD, String.valueOf(config.keyStorePassword));
      }

      if (config.cipherSuites.size() > 0) {
        driverConfigLoaderBuilder
            .withStringList(DefaultDriverOption.SSL_CIPHER_SUITES, config.cipherSuites);
      }
    }

    driverConfigLoaderBuilder.withString(DefaultDriverOption.PROTOCOL_COMPRESSION, config.compression);

    log.info("Creating session");
    sessionBuilder.withConfigLoader(driverConfigLoaderBuilder.build());
    final CqlSession session = sessionBuilder.build();
    return new ScyllaDbSessionImpl(config, session);
  }

  private void configurePublicContactPoints(ScyllaDbSinkConnectorConfig config, CqlSessionBuilder sessionBuilder) {
    log.info("Configuring public contact points={}", config.contactPoints);
    String[] contactPointsArray = config.contactPoints.split(",");
    for (String contactPoint : contactPointsArray) {
      if (contactPoint == null) {
        throw new NullPointerException("One of provided contact points is null");
      }
      sessionBuilder.addContactPoint(new InetSocketAddress(contactPoint, config.port));
    }
  }

  private void configureAddressTranslator(ScyllaDbSinkConnectorConfig config, CqlSessionBuilder sessionBuilder, ProgrammaticDriverConfigLoaderBuilder configBuilder) throws JsonProcessingException {
    log.info("Trying to configure address translator for private network address and port.");
    ClusterAddressTranslator translator = new ClusterAddressTranslator();
    translator.setMap(config.contactPoints);
    sessionBuilder.addContactPoints(translator.getContactPoints());
    configBuilder.withClass(DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS, ClusterAddressTranslator.class);
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
