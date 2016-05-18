package com.adaptris.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import com.adaptris.core.CoreException;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Implementation of {@link ProducerConfigBuilder} that exposes all configuration.
 * 
 * <p>
 * Exposes all possible settings via a {@link KeyValuePairSet}. No checking of values is performed other than for the various
 * SSL passwords (such as {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}) which will be decoded using
 * {@link Password#decode(String)} appropriately.
 * </p>
 * <p>
 * Regardless of what is configured; the {@code key.serializer} property is
 * fixed to be a {@link StringSerializer}; and the {@code value.serializer} property is always an {@link AdaptrisMessageSerializer}.
 * </p>
 * 
 * @author lchan
 * @config kafka-advanced-producer-config
 */
@XStreamAlias("kafka-advanced-producer-config")
public class AdvancedProducerConfigBuilder extends AdvancedConfigBuilderImpl implements ProducerConfigBuilder {


  public AdvancedProducerConfigBuilder() {
    super();
  }

  public AdvancedProducerConfigBuilder(KeyValuePairSet cfg) {
    super(cfg);
  }

  @Override
  public Map<String, Object> build() throws CoreException {
    Map<String, Object> result = convertAndDecode(getConfig());
    result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER);
    return result;
  }
}