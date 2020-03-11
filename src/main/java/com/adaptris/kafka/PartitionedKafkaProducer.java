package com.adaptris.kafka;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.validation.constraints.NumberExpression;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Extension of {@link StandardKafkaProducer} that allows you to specify a partition.
 * 
 * 
 * @config partitioned-apache-kafka-producer
 * 
 */
@XStreamAlias("partitioned-apache-kafka-producer")
@ComponentProfile(summary = "Produce messages to Apache Kafka with a specific partition number", tag = "producer,kafka", recommended =
{
    KafkaConnection.class
})
@DisplayOrder(order = {"recordKey", "partition"})
public class PartitionedKafkaProducer extends StandardKafkaProducer {

  @InputFieldHint(expression = true)
  @NumberExpression
  private String partition;

  public PartitionedKafkaProducer() {
    super();
  }

  public PartitionedKafkaProducer(String recordKey, ProduceDestination d) {
    this();
    setRecordKey(recordKey);
    setDestination(d);
  }

  @Override
  protected ProducerRecord<String, AdaptrisMessage> createProducerRecord(String topic, String key, AdaptrisMessage msg) {
    Integer targetPartition = toInt(msg.resolve(getPartition()));
    log.trace("Sending message [{}] to topic [{}][partition={}] with key [{}]", msg.getUniqueId(), topic, targetPartition, key);
    return new ProducerRecord<String, AdaptrisMessage>(topic, targetPartition, key, msg);
  }

  private static Integer toInt(String s) {
    Integer result = null;
    try {
      result = NumberUtils.createInteger(s);
    }
    catch (NumberFormatException e) {
      // if we get %message{key} - and it doesn't exist, then we'll get a NFE
      // if we get %message{key} -> key then we'll get a NFE
      // if we get %message{key} -> 0F then we'll get 15 (which might still cause problems)
    }
    return result;
  }

  public String getPartition() {
    return partition;
  }

  /**
   * Set the partition.
   * 
   * @param s the partition; can be of the form {@code %message{key1}} to use the metadata value associated with {@code key1}. If it
   *          doesn't resolve to an Integer; then {@code null} is used.
   */
  public void setPartition(String s) {
    this.partition = s;
  }

  public <T extends PartitionedKafkaProducer> T withPartition(String s) {
    setPartition(s);
    return (T) this;
  }
}
