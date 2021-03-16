package com.adaptris.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import com.adaptris.kafka.ConfigBuilder.Acks;
import com.adaptris.kafka.ConfigBuilder.CompressionType;

public class SimpleConfigBuilderTest {

  @Test
  public void testBootstrapServer() {
    SimpleConfigBuilder builder = new SimpleConfigBuilder();
    assertNull(builder.getBootstrapServers());
    builder.setBootstrapServers("localhost:4242");
    assertEquals("localhost:4242", builder.getBootstrapServers());
    try {
      builder.setBootstrapServers(null);
      fail();
    } catch (IllegalArgumentException expected) {

    }
    assertEquals("localhost:4242", builder.getBootstrapServers());

  }

  @Test
  public void testGroupId() {
    SimpleConfigBuilder builder = new SimpleConfigBuilder();
    assertNull(builder.getGroupId());
    builder.setGroupId("myGroup");
    assertEquals("myGroup", builder.getGroupId());
    builder.setGroupId(null);
    assertNull(builder.getGroupId());
  }

  @Test
  public void testBuild_Minimal() throws Exception {
    SimpleConfigBuilder builder = new SimpleConfigBuilder();
    builder.setBootstrapServers("localhost:4242");
    Map<String, Object> p = builder.build();
    assertEquals("localhost:4242", p.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(StringDeserializer.class.getName(), p.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals(AdaptrisMessageDeserializer.class.getName(), p.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    assertNull(p.get(ConsumerConfig.GROUP_ID_CONFIG));
  }

  @Test
  public void testBuild_Complete() throws Exception {
    SimpleConfigBuilder builder = new SimpleConfigBuilder();
    builder.setBootstrapServers("localhost:4242");
    builder.setAcks(Acks.none);
    builder.setBufferMemory(1024L);
    builder.setCompressionType(CompressionType.lz4);
    builder.setRetries(1);
    builder.setGroupId("myGroup");
    Map<String, Object> p = builder.build();
    assertEquals(StringDeserializer.class.getName(), p.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals(AdaptrisMessageDeserializer.class.getName(), p.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    assertEquals(StringSerializer.class.getName(), p.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals(AdaptrisMessageSerializer.class.getName(), p.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

    assertEquals("localhost:4242", p.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("myGroup", p.get(ConsumerConfig.GROUP_ID_CONFIG));
    assertEquals("0", p.get(ProducerConfig.ACKS_CONFIG));
    assertEquals(1, p.get(ProducerConfig.RETRIES_CONFIG));
    assertEquals("lz4", p.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    assertEquals(1024L, p.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
  }
}
