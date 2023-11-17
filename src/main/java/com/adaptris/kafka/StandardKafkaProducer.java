package com.adaptris.kafka;

import java.util.Map;

import javax.validation.constraints.NotBlank;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProduceOnlyProducerImp;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.kafka.ConfigDefinition.FilterKeys;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.Setter;

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
@ComponentProfile(summary = "Deliver messages via Apache Kafka", tag = "producer,kafka", recommended = { KafkaConnection.class })
@DisplayOrder(order = { "topic", "recordKey" })
public class StandardKafkaProducer extends ProduceOnlyProducerImp {

  @NotBlank
  @InputFieldHint(expression = true)
  private String recordKey;

  /**
   * The Kafka Topic to produce to
   *
   * @paran topic
   */
  @NotBlank
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  private String topic;

  protected transient KafkaProducer<String, AdaptrisMessage> producer;

  public StandardKafkaProducer() {
  }

  public StandardKafkaProducer(String recordKey, String topic) {
    setRecordKey(recordKey);
    setTopic(topic);
  }

  @Override
  public void init() throws CoreException {
    try {
      Args.notBlank(getRecordKey(), "record-key");
    } catch (IllegalArgumentException e) {
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
  public void close() {
  }

  @Override
  public void prepare() throws CoreException {
    Args.notNull(getTopic(), "topic");
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String topic) throws ProduceException {
    try {
      String key = msg.resolve(getRecordKey());
      producer.send(createProducerRecord(topic, key, msg));
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  private Map<String, Object> buildConfig() throws CoreException {
    return retrieveConnection(KafkaConnection.class).buildConfig(FilterKeys.Producer);
  }

  protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
    return new KafkaProducer<>(config);
  }

  protected ProducerRecord<String, AdaptrisMessage> createProducerRecord(String topic, String key, AdaptrisMessage msg) {
    log.trace("Sending message [{}] to topic [{}] with key [{}]", msg.getUniqueId(), topic, key);
    return new ProducerRecord<>(topic, key, msg);
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
    recordKey = Args.notNull(k, "key");
  }

  @Override
  public void registerConnection(AdaptrisConnection conn) {
    super.registerConnection(conn);
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getTopic());
  }
}
