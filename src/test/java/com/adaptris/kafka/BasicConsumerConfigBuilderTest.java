package com.adaptris.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

public class BasicConsumerConfigBuilderTest {

  @Test
  public void testBootstrapServer() {
    BasicConsumerConfigBuilder builder = new BasicConsumerConfigBuilder();
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
    BasicConsumerConfigBuilder builder = new BasicConsumerConfigBuilder();
    assertNull(builder.getGroupId());
    builder.setGroupId("myGroup");
    assertEquals("myGroup", builder.getGroupId());
    builder.setGroupId(null);
    assertNull(builder.getGroupId());
  }

  @Test
  public void testBuild() throws Exception {
    BasicConsumerConfigBuilder builder = new BasicConsumerConfigBuilder();
    builder.setBootstrapServers("localhost:4242");
    Map<String, Object> p = builder.build();
    assertEquals("localhost:4242", p.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(StringDeserializer.class.getName(), p.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals(AdaptrisMessageDeserializer.class.getName(), p.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    assertNull(p.get(ConsumerConfig.GROUP_ID_CONFIG));
  }

}
