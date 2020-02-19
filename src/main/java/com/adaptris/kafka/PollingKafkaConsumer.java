package com.adaptris.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisPollingConsumer;
import com.adaptris.core.ConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullConnection;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.TimeInterval;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wrapper around {@link KafkaConsumer}.
 * 
 * 
 * @author lchan
 * @config polling-apache-kafka-consumer
 * 
 */
@XStreamAlias("polling-apache-kafka-consumer")
@ComponentProfile(summary = "Receive messages via Apache Kafka", tag = "consumer,kafka", recommended = {NullConnection.class})
@DisplayOrder(order = {"destination", "consumerConfig", "receiveTimeout", "additionalDebug"})
public class PollingKafkaConsumer extends AdaptrisPollingConsumer implements LoggingContext {

  private static final TimeInterval DEFAULT_RECV_TIMEOUT_INTERVAL = new TimeInterval(2L, TimeUnit.SECONDS);

  @NotNull
  @Valid
  private ConsumerConfigBuilder consumerConfig;
  @AdvancedConfig
  private TimeInterval receiveTimeout;
  @AdvancedConfig
  private Boolean additionalDebug;

  private transient KafkaConsumer<String, AdaptrisMessage> consumer;

  public PollingKafkaConsumer() {
    setConsumerConfig(new BasicConsumerConfigBuilder());
  }

  public PollingKafkaConsumer(ConsumeDestination d, ConsumerConfigBuilder b) {
    setConsumerConfig(b);
    setDestination(d);
  }



  @Override
  public void init() throws CoreException {
    super.init();
  }

  @Override
  public void start() throws CoreException {
    try {
      Map<String, Object> props = getConsumerConfig().build();
      props.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, getMessageFactory());
      consumer = createConsumer(props);
      List<String> topics = Arrays.asList(Args.notBlank(getDestination().getDestination(), "topics").split("\\s*,\\s*"));
      LoggingContext.LOGGER.logPartitions(this, topics, consumer);
      consumer.subscribe(topics);
    } catch (RuntimeException e) {
      // ConfigException extends KafkaException which is a RTE
      throw ExceptionHelper.wrapCoreException(e);
    }
    super.start();
  }

  @Override
  public void stop() {
    closeConsumer();
    super.stop();
  }

  @Override
  public void close() {
    closeConsumer();
    super.close();
  }


  @Override
  protected void prepareConsumer() throws CoreException {}

  private void closeConsumer() {
    try {
      if (consumer != null) {
        consumer.wakeup();
        consumer.close();
        consumer = null;
      }
    } catch (RuntimeException e) {

    }

  }

  @Override
  protected int processMessages() {
    int proc = 0;
    try {
      log.trace("Going to Poll with timeout {}", receiveTimeoutMs());
      ConsumerRecords<String, AdaptrisMessage> records = consumer.poll(Duration.ofMillis(receiveTimeoutMs()));
      for (ConsumerRecord<String, AdaptrisMessage> record : records) {
        retrieveAdaptrisMessageListener().onAdaptrisMessage(record.value());
        proc++;
      }
    } catch (Exception e) {
      log.warn("Exception during poll(), waiting for next scheduled poll");
      if (additionalDebug()) {
        log.warn(e.getMessage(), e);
      }
    }

    return proc;
  }

  public ConsumerConfigBuilder getConsumerConfig() {
    return consumerConfig;
  }

  public void setConsumerConfig(ConsumerConfigBuilder pc) {
    this.consumerConfig = Args.notNull(pc, "consumer-config");
  }

  long receiveTimeoutMs() {
    return getReceiveTimeout() != null ? getReceiveTimeout().toMilliseconds() : DEFAULT_RECV_TIMEOUT_INTERVAL.toMilliseconds();
  }

  public TimeInterval getReceiveTimeout() {
    return receiveTimeout;
  }

  KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
    return new KafkaConsumer<String, AdaptrisMessage>(config);
  }

  /**
   * @return the logAllExceptions
   */
  public Boolean getAdditionalDebug() {
    return additionalDebug;
  }

  /**
   * Whether or not to log all stacktraces.
   *
   * @param b the logAllExceptions to set, default false
   */
  public void setAdditionalDebug(Boolean b) {
    additionalDebug = b;
  }

  @Override
  public boolean additionalDebug() {
    return getAdditionalDebug() != null ? getAdditionalDebug().booleanValue() : false;
  }

  @Override
  public Logger logger() {
    return log;
  }

  /**
   * Set the receive timeout.
   * 
   * @param rt the receive timout.
   */
  public void setReceiveTimeout(TimeInterval rt) {
    this.receiveTimeout = rt;
  }

}
