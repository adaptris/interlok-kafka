package com.adaptris.kafka;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

import org.apache.kafka.clients.producer.ProducerConfig;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Basic implementation of {@link ProducerConfigBuilder}.
 *
 * <p>
 * Only "high" importance properties from <a href="http://kafka.apache.org/documentation.html#producerconfigs">the Apache Kafka
 * Producer Config Documentation</a> are exposed; all other properties are left as default. The {@code key.serializer} property is
 * fixed to be a {@code StringSerializer}; and the {@code value.serializer} property is always an {@link AdaptrisMessageSerializer}.
 * </p>
 *
 * @author lchan
 * @config kafka-basic-producer-config
 * @deprecated since 3.8.0 when using a {@link KafkaConnection}; has no meaning.
 */
@XStreamAlias("kafka-basic-producer-config")
@DisplayOrder(order = {"bootstrapServers", "acks", "retries", "compressionType", "bufferMemory"})
@Deprecated
public class BasicProducerConfigBuilder implements ProducerConfigBuilder {

  static final long DEFAULT_BUFFER_MEM = 33554432L;
  static final int DEFAULT_RETRIES = 0;
  private static final CompressionType DEFAULT_COMPRESSION_TYPE = ProducerConfigBuilder.CompressionType.none;
  private static final Acks DEFAULT_ACKS = ProducerConfigBuilder.Acks.all;


  @NotBlank
  private String bootstrapServers;
  @AdvancedConfig
  private Long bufferMemory;
  @AdvancedConfig
  private CompressionType compressionType;
  @AdvancedConfig
  private Acks acks;
  @AdvancedConfig
  @Min(0)
  private Integer retries;

  public BasicProducerConfigBuilder() {

  }

  public BasicProducerConfigBuilder(String bootstrapServers) {
    this();
    setBootstrapServers(bootstrapServers);
  }

  @Override
  public Map<String, Object> build(KeyFilter filter) throws CoreException {
    Map<String, Object> props = new HashMap<>();
    try {
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
      props.put(ProducerConfig.ACKS_CONFIG, acks());
      props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType());
      props.put(ProducerConfig.RETRIES_CONFIG, retries());
      props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory());
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER);
      props.keySet().retainAll(filter.retainKeys());
    } catch (

        IllegalArgumentException e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
    return props;
  }


  public String getBootstrapServers() {
    return bootstrapServers;
  }


  /**
   * Set the {@code bootstrap.servers} property.
   * <p>
   * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all
   * servers irrespective of which servers are specified here for bootstrapping; this list only impacts the initial hosts used to
   * discover the full set of servers. This list should be in the form {@code host1:port1,host2:port2,....}. Since these servers are
   * just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not
   * contain the full set of servers (you may want more than one, though, in case a server is down).
   * </p>
   *
   * @param s the bootstrap servers
   */
  public void setBootstrapServers(String s) {
    bootstrapServers = Args.notBlank(s, "bootstrap-servers");
  }

  public Long getBufferMemory() {
    return bufferMemory;
  }

  long bufferMemory() {
    return getBufferMemory() != null ? getBufferMemory().longValue() : DEFAULT_BUFFER_MEM;
  }

  /**
   * Set the {@code buffer.memory} property.
   * <p>
   * The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are sent faster
   * than they can be delivered to the server the producer will either block or throw an exception based on the preference specified
   * by block.on.buffer.full ({code block.on.buffer.full} defaults to false, so an exception will be thrown).
   * </p>
   * <p>
   * This setting should correspond roughly to the total memory the producer will use, but is not a hard bound since not all memory
   * the producer uses is used for buffering. Some additional memory will be used for compression (if compression is enabled) as
   * well as for maintaining in-flight requests.
   * </p>
   *
   * @param m the buffer memory; default is 33554432L if not specified.
   */
  public void setBufferMemory(Long m) {
    bufferMemory = m;
  }


  public CompressionType getCompressionType() {
    return compressionType;
  }

  /**
   * Set the {@code compression.type} property.
   * <p>
   * The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid values are none,
   * gzip, snappy, or lz4. Compression is of full batches of data, so the efficacy of batching will also impact the compression
   * ratio (more batching means better compression).
   * </p>
   *
   * @param t the compression type; default is {@link ConfigBuilder.CompressionType#none} if not specified.
   */
  public void setCompressionType(CompressionType t) {
    compressionType = t;
  }


  String compressionType() {
    return getCompressionType() != null ? getCompressionType().name() : DEFAULT_COMPRESSION_TYPE.name();
  }

  public Integer getRetries() {
    return retries;
  }


  /**
   * Set the {@code retries} property.
   * <p>
   * Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient
   * error. Note that this retry is no different than if the client resent the record upon receiving the error. Allowing retries
   * will potentially change the ordering of records because if two records are sent to a single partition, and the first fails and
   * is retried but the second succeeds, then the second record may appear first.
   * </p>
   *
   * @param i the number of retries, default is 0 if not specified.
   */
  public void setRetries(Integer i) {
    retries = i;
  }

  int retries() {
    return getRetries() != null ? getRetries().intValue() : DEFAULT_RETRIES;
  }

  public Acks getAcks() {
    return acks;
  }


  /**
   * Set the {@code acks} property.
   *
   * <p>
   * This specifies number of acknowledgments the producer requires the leader to have received before considering a request
   * complete
   * </p>
   *
   * @param a the number of acks; default is {@link ConfigBuilder.Acks#all} if not specified for the strongest available guarantee.
   */
  public void setAcks(Acks a) {
    acks = a;
  }

  String acks() {
    return getAcks() != null ? getAcks().actualValue() : DEFAULT_ACKS.actualValue();
  }

}
