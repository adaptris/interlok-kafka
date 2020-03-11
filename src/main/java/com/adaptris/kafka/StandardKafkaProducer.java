package com.adaptris.kafka;

import java.util.Map;

import javax.validation.constraints.NotBlank;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.kafka.ConfigDefinition.FilterKeys;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wrapper around {@link KafkaProducer}.
 * 
 * 
 * @author gdries
 * @author lchan
 * @config standard-apache-kafka-producer
 * 
 */
@XStreamAlias("standard-apache-kafka-producer")
@ComponentProfile(summary = "Deliver messages via Apache Kafka", tag = "producer,kafka", recommended = {KafkaConnection.class})
@DisplayOrder(order = {"recordKey"})
public class StandardKafkaProducer extends ProduceOnlyProducerImp {

  @NotBlank
  @InputFieldHint(expression = true)
  private String recordKey;
  @Deprecated
  @AdvancedConfig
  private ProducerConfigBuilder producerConfig;

  protected transient KafkaProducer<String, AdaptrisMessage> producer;
  protected transient boolean configFromConnection;

  public StandardKafkaProducer() {
  }

  public StandardKafkaProducer(String recordKey, ProduceDestination d) {
    setRecordKey(recordKey);
    setDestination(d);
  }

  @Deprecated
  StandardKafkaProducer(String recordKey, ProduceDestination d, ProducerConfigBuilder b) {
    this();
    setRecordKey(recordKey);
    setDestination(d);
    setProducerConfig(b);
  }

  @Override
  public void init() throws CoreException {
    try {
      Args.notBlank(getRecordKey(), "record-key");
    }
    catch (IllegalArgumentException e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
    producer = null;
  }

  @Override
  public void start() throws CoreException {
    try {
      producer = createProducer(buildConfig());
    } catch (RuntimeException e) {
      // ConfigException extends KafkaException which is a RTE
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
      producer = null;
    }
  }

  @Override
  public void close() {}

  @Override
  public void prepare() throws CoreException {}

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    try {
      String topic = destination.getDestination(msg);
      String key = msg.resolve(getRecordKey());
      producer.send(createProducerRecord(topic, key, msg));
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  private Map<String, Object> buildConfig() throws CoreException {
    if (configFromConnection) {
      return retrieveConnection(KafkaConnection.class).buildConfig(FilterKeys.Producer);
    }
    log.warn("producer-config is deprecated); use a {} instead", KafkaConnection.class.getSimpleName());
    return getProducerConfig().build();
  }

  protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
    return new KafkaProducer<String, AdaptrisMessage>(config);
  }

  protected ProducerRecord<String, AdaptrisMessage> createProducerRecord(String topic, String key, AdaptrisMessage msg) {
    log.trace("Sending message [{}] to topic [{}] with key [{}]", msg.getUniqueId(), topic, key);
    return new ProducerRecord<String, AdaptrisMessage>(topic, key, msg);
  }

  /**
   * 
   * @deprecated since 3.7.0 use a {@link KafkaConnection} instead.
   */
  @Deprecated
  public ProducerConfigBuilder getProducerConfig() {
    return producerConfig;
  }

  /**
   * 
   * @deprecated since 3.7.0 use a {@link KafkaConnection} instead.
   */
  @Deprecated
  public void setProducerConfig(ProducerConfigBuilder pc) {
    this.producerConfig = pc;
  }

  public String getRecordKey() {
    return recordKey;
  }

  /**
   * Set the key for the generated {@link ProducerRecord}.
   * 
   * @param k
   */
  public void setRecordKey(String k) {
    this.recordKey = Args.notNull(k, "key");
  }

  @Override
  public void registerConnection(AdaptrisConnection conn) {
    super.registerConnection(conn);
    if (conn instanceof KafkaConnection) {
      configFromConnection = true;
    }

  }
}
