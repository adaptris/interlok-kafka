package com.adaptris.kafka;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Implementation of {@link ConsumerConfigBuilder} that exposes all configuration.
 *
 * <p>
 * Exposes all possible settings via a {@link KeyValuePairSet}. No checking of values is performed other than for the various SSL
 * passwords (such as {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}) which will be decoded using {@link Password#decode(String)}
 * appropriately.
 * </p>
 * <p>
 * Regardless of what is configured; the {@code key.deserializer} property is fixed to be a {@link StringDeserializer}; and the
 * {@code value.deserializer} property is always an {@link AdaptrisMessageDeserializer}.
 * </p>
 *
 * @author lchan
 * @config kafka-advanced-consumer-config
 */
@XStreamAlias("kafka-advanced-consumer-config")
@ComponentProfile(summary = "Advanced implementation of a Kafka consumer config builder", tag = "builder,kafka", since = "3.6.6")
public class AdvancedConsumerConfigBuilder extends AdvancedConfigBuilder implements ConsumerConfigBuilder {

  public AdvancedConsumerConfigBuilder() {
    super();
  }


  public AdvancedConsumerConfigBuilder(KeyValuePairSet cfg) {
    super(cfg);
  }
}
