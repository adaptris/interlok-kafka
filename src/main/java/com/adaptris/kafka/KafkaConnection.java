package com.adaptris.kafka;

import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.CoreException;
import com.adaptris.core.NoOpConnection;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wraps the {@code Map<String,Object>} object used to create {@code KafkaConsumer} {@code KafkaProducer} instances.
 * 
 * 
 * @author lchan
 * @config apache-kafka-connection
 *
 */
@XStreamAlias("apache-kafka-connection")
@ComponentProfile(summary = "Connection to Apache Kafka", tag = "connections,kafka")
@DisplayOrder(order =
{
    "configBuilder"
})
public class KafkaConnection extends NoOpConnection {

  @AutoPopulated
  @Valid
  @NotNull
  private ConfigBuilder configBuilder;

  public KafkaConnection() {
    super();
  }

  public KafkaConnection(ConfigBuilder builder) {
    this();
    setConfigBuilder(builder);
  }

  public ConfigBuilder getConfigBuilder() {
    return configBuilder;
  }

  public void setConfigBuilder(ConfigBuilder configBuilder) {
    this.configBuilder = Args.notNull(configBuilder, "configBuilder");
  }

  public Map<String, Object> buildConfig(ConfigBuilder.KeyFilter filter) throws CoreException {
    return getConfigBuilder().build(filter);
  }
}
