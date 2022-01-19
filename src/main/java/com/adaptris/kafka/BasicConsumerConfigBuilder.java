package com.adaptris.kafka;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotBlank;

import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.Setter;

/**
 * Basic implementation of {@link ConsumerConfigBuilder}.
 *
 * <p>
 * Only "high" importance properties from <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">the Apache Kafka
 * Consumer Config Documentation</a> are exposed; all other properties are left as default. The {@code key.serializer} property is
 * fixed to be a {@link StringSerializer}; and the {@code value.serializer} property is always an {@link AdaptrisMessageSerializer}.
 * </p>
 *
 * @author lchan
 * @config kafka-basic-consumer-config
 */
@XStreamAlias("kafka-basic-consumer-config")
@ComponentProfile(summary = "Basic implementation of a Kafka consumer config builder", tag = "builder,kafka", since = "3.6.6")
@DisplayOrder(order = {"bootstrapServers", "groupId"})
public class BasicConsumerConfigBuilder extends ConfigBuilderImpl implements ConsumerConfigBuilder {

  /**
   * Set the {@code bootstrap.servers} property.
   * <p>
   * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers
   * irrespective of which servers are specified here for bootstrapping; this list only impacts the initial hosts used to discover the full
   * set of servers. This list should be in the form {@code host1:port1,host2:port2,....}. Since these servers are just used for the initial
   * connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers
   * (you may want more than one, though, in case a server is down).
   * </p>
   *
   * @param bootstrapServers
   *          the bootstrap servers
   */
  @NotBlank
  @NonNull
  @Getter
  @Setter
  private String bootstrapServers;

  /**
   * Set the {@code group.id} property.
   * <p>
   * A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the
   * group management functionality by using subscribe(topic) or the Kafka-based offset management strategy.
   * </p>
   *
   * @param groupId
   *          the groupId to set
   */
  @Getter
  @Setter
  private String groupId;

  public BasicConsumerConfigBuilder() {
  }

  public BasicConsumerConfigBuilder(String bootstrapServers) {
    this();
    setBootstrapServers(bootstrapServers);
  }

  public BasicConsumerConfigBuilder(String bootstrapServers, String groupId) {
    this();
    setBootstrapServers(bootstrapServers);
    setGroupId(groupId);
  }

  @Override
  public Map<String, Object> build(KeyFilter filter) throws CoreException {
    Map<String, Object> props = new HashMap<>();
    try {
      Args.notBlank(getBootstrapServers(), "bootstrapServers");
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
      addEntry(props, ConsumerConfig.GROUP_ID_CONFIG, getGroupId());
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
      log.trace("Keeping Config Keys : {}", filter.retainKeys());
      props.keySet().retainAll(filter.retainKeys());
    }
    catch (IllegalArgumentException e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
    return props;
  }

}
