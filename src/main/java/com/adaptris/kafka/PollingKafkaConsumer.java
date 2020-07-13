package com.adaptris.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisPollingConsumer;
import com.adaptris.core.ConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullConnection;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.DestinationHelper;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.util.TimeInterval;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

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
@DisplayOrder(
    order = {"topics", "destination", "consumerConfig", "receiveTimeout", "additionalDebug"})
public class PollingKafkaConsumer extends AdaptrisPollingConsumer implements LoggingContext {

  private static final TimeInterval DEFAULT_RECV_TIMEOUT_INTERVAL = new TimeInterval(2L, TimeUnit.SECONDS);

  @NotNull
  @Valid
  private ConsumerConfigBuilder consumerConfig;
  @AdvancedConfig
  private TimeInterval receiveTimeout;
  @AdvancedConfig
  private Boolean additionalDebug;

  /**
   * The consume destination contains the topics we want to consume from.
   *
   */
  @Getter
  @Setter
  @Deprecated
  @Valid
  @Removal(version = "4.0.0", message = "Use 'topics' instead")
  private ConsumeDestination destination;

  /**
   * A comma separated list of topics that you want to consume from.
   *
   */
  @Getter
  @Setter
  // Needs to be @NotBlank when destination is removed.
  private String topics;
  private transient boolean destinationWarningLogged;

  private transient KafkaConsumer<String, AdaptrisMessage> consumer;

  public PollingKafkaConsumer() {
    setConsumerConfig(new BasicConsumerConfigBuilder());
  }

  public PollingKafkaConsumer(ConsumerConfigBuilder b) {
    setConsumerConfig(b);
  }

  @Override
  public void start() throws CoreException {
    try {
      Map<String, Object> props = getConsumerConfig().build();
      props.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, getMessageFactory());
      consumer = createConsumer(props);
      List<String> topics = Arrays.asList(Args.notBlank(topics(), "topics").split("\\s*,\\s*"));
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
      logger().trace("Going to Poll with timeout {}", receiveTimeoutMs());
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
    consumerConfig = Args.notNull(pc, "consumer-config");
  }

  long receiveTimeoutMs() {
    return TimeInterval.toMillisecondsDefaultIfNull(getReceiveTimeout(),
        DEFAULT_RECV_TIMEOUT_INTERVAL);
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
    return BooleanUtils.toBooleanDefaultIfNull(getAdditionalDebug(), false);
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
    receiveTimeout = rt;
  }

  public PollingKafkaConsumer withTopics(String s) {
    setTopics(s);
    return this;
  }

  @Override
  protected void prepareConsumer() throws CoreException {
    DestinationHelper.logConsumeDestinationWarning(destinationWarningLogged,
        () -> destinationWarningLogged = true, getDestination(),
        "{} uses destination, use topics instead", LoggingHelper.friendlyName(this));
    DestinationHelper.mustHaveEither(getTopics(), getDestination());
  }

  private String topics() {
    return DestinationHelper.consumeDestination(getTopics(), getDestination());
  }

  @Override
  protected String newThreadName() {
    return DestinationHelper.threadName(retrieveAdaptrisMessageListener(), getDestination());
  }

}
