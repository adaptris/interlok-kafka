package com.adaptris.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.kafka.ConfigDefinition.FilterKeys;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Implementation of {@link ConfigBuilder} that exposes all configuration.
 * 
 * <p>
 * Exposes all possible settings via a {@link KeyValuePairSet}. No checking of values is performed other than for the various SSL
 * passwords (such as {@value SslConfigs#SSL_KEY_PASSWORD_CONFIG}) which will be resolved and decoded using
 * {@link ExternalResolver#resolve(String)} and {@link Password#decode(String)} appropriately. Because no checking is done, it will
 * be possible to get some warnings about unused configuration (e.g. you have configured {@code linger.ms} on a connection that is
 * used for both producers and consumer) being logged. These can be safely ignored or filtered from logging (filter the classes
 * {@code org.apache.kafka.clients.consumer.ConsumerConfig} and {code org.apache.kafka.clients.producer.ProducerConfig}
 * </p>
 * <p>
 * If not explicitly configured; the {@code key.deserializer} property is fixed to be a {@code StringDeserializer}; and the
 * {@code value.deserializer} property is an {@link AdaptrisMessageDeserializer}.
 * </p>
 * 
 * @config kafka-advanced-config-builder
 * 
 */
@XStreamAlias("kafka-advanced-config-builder")
public class AdvancedConfigBuilder implements ConfigBuilder {
  protected transient Logger log = LoggerFactory.getLogger(this.getClass());
  /**
   * List of keys that can be {@code encoded} via {@link Password#decode(String)} and externally resolved via
   * {@link ExternalResolver#resolve(String)}.
   * <p>
   * Currently set to : {@code ssl.key.password, ssl.keystore.password, ssl.truststore.password}
   * </p>
   */
  public static final List<String> PASSWORD_KEYS = Arrays.asList(new String[]
  {
      SslConfigs.SSL_KEY_PASSWORD_CONFIG, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
  });


  @NotNull
  @Valid
  @AutoPopulated
  private KeyValuePairSet config;


  public AdvancedConfigBuilder() {
    setConfig(new KeyValuePairSet());
  }

  public AdvancedConfigBuilder(KeyValuePairSet cfg) {
    setConfig(cfg);
  }

  /**
   * @return the config
   */
  public KeyValuePairSet getConfig() {
    return config;
  }

  /**
   * @param config the config to set
   */
  public void setConfig(KeyValuePairSet config) {
    this.config = Args.notNull(config, "config");
  }

  @Override
  public Map<String, Object> build() throws CoreException {
    return build(FilterKeys.ProducerOrConsumer);
  }

  @Override
  public Map<String, Object> build(KeyFilter f) throws CoreException {
    final Map<String, Object> result = convertAndDecode(getConfig());
    result.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    result.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER);
    result.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
    result.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
    log.trace("Keeping Config Keys : {}", f.retainKeys());
    result.keySet().retainAll(f.retainKeys());
    return result;
  }

  private static Map<String, Object> convertAndDecode(KeyValuePairSet kvps) throws CoreException {
    Map<String, Object> result = new HashMap<>();
    try {
      for (KeyValuePair kvp : kvps.getKeyValuePairs()) {
        if (PASSWORD_KEYS.contains(kvp.getKey())) {
          result.put(kvp.getKey(), Password.decode(ExternalResolver.resolve(kvp.getValue())));
        } else {
          result.put(kvp.getKey(), kvp.getValue());
        }
      }
    } catch (PasswordException e) {
      throw new CoreException(e);
    }
    return result;
  }
}
