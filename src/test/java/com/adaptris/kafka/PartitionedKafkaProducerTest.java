package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;

public class PartitionedKafkaProducerTest {

  @Test
  public void testRecordResolvesPartitionFromMetadata() {
    PartitionedKafkaProducer producer = new PartitionedKafkaProducer();
    assertSame(producer, producer.withPartition("%message{partition}"));
    AdaptrisMessage message = AdaptrisMessageFactory.getDefaultInstance().newMessage("payload");
    message.addMetadata("partition", "3");

    ProducerRecord<String, AdaptrisMessage> record = producer.createProducerRecord("topic", "key", message);

    assertEquals("topic", record.topic());
    assertEquals("key", record.key());
    assertEquals(Integer.valueOf(3), record.partition());
    assertSame(message, record.value());
  }

  @Test
  public void testRecordWithoutPartitionUsesKafkaPartitionSelection() {
    PartitionedKafkaProducer producer = new PartitionedKafkaProducer();
    AdaptrisMessage message = AdaptrisMessageFactory.getDefaultInstance().newMessage("payload");

    ProducerRecord<String, AdaptrisMessage> record = producer.createProducerRecord("topic", "key", message);

    assertNull(record.partition());
    assertSame(message, record.value());
  }

  @Test
  public void testRecordWithInvalidPartitionUsesKafkaPartitionSelection() {
    PartitionedKafkaProducer producer = new PartitionedKafkaProducer().withPartition("not-a-number");
    AdaptrisMessage message = AdaptrisMessageFactory.getDefaultInstance().newMessage("payload");

    ProducerRecord<String, AdaptrisMessage> record = producer.createProducerRecord("topic", "key", message);

    assertNull(record.partition());
    assertSame(message, record.value());
  }
}
