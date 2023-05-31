package com.adaptris.kafka;

import static com.adaptris.kafka.BasicProducerConfigBuilder.DEFAULT_BUFFER_MEM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import com.adaptris.kafka.ConfigBuilder.Acks;
import com.adaptris.kafka.ConfigBuilder.CompressionType;

@SuppressWarnings("deprecation")
public class BasicProducerConfigBuilderTest {

  @Test
  public void testBootstrapServer() {
    BasicProducerConfigBuilder builder = new BasicProducerConfigBuilder();
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
  public void testBufferMemory() {
    BasicProducerConfigBuilder builder = new BasicProducerConfigBuilder();
    assertNull(builder.getBufferMemory());
    assertEquals(DEFAULT_BUFFER_MEM, builder.bufferMemory());
    builder.setBufferMemory(1234L);
    assertEquals(Long.valueOf(1234L), builder.getBufferMemory());
    assertEquals(1234L, builder.bufferMemory());
    builder.setBufferMemory(null);
    assertNull(builder.getBufferMemory());
    assertEquals(DEFAULT_BUFFER_MEM, builder.bufferMemory());
  }

  @Test
  public void testCompressionType() {
    BasicProducerConfigBuilder builder = new BasicProducerConfigBuilder();
    assertNull(builder.getCompressionType());
    assertEquals(CompressionType.none.name(), builder.compressionType());
    builder.setCompressionType(CompressionType.lz4);
    assertEquals(CompressionType.lz4, builder.getCompressionType());
    assertEquals(CompressionType.lz4.name(), builder.compressionType());
    builder.setCompressionType(null);
    assertNull(builder.getCompressionType());
    assertEquals(CompressionType.none.name(), builder.compressionType());
  }

  @Test
  public void testAcks() {
    BasicProducerConfigBuilder builder = new BasicProducerConfigBuilder();
    assertNull(builder.getAcks());
    assertEquals(Acks.all.actualValue(), builder.acks());
    builder.setAcks(Acks.local);
    assertEquals(Acks.local, builder.getAcks());
    assertEquals(Acks.local.actualValue(), builder.acks());
    builder.setAcks(null);
    assertNull(builder.getAcks());
    assertEquals(Acks.all.actualValue(), builder.acks());
  }

  @Test
  public void testRetries() {
    BasicProducerConfigBuilder builder = new BasicProducerConfigBuilder();
    assertNull(builder.getRetries());
    assertEquals(0, builder.retries());
    builder.setRetries(1);
    assertEquals(Integer.valueOf(1), builder.getRetries());
    assertEquals(1, builder.retries());
    builder.setRetries(null);
    assertNull(builder.getRetries());
    assertEquals(0, builder.retries());
  }

  @Test
  public void testBuild() throws Exception {
    BasicProducerConfigBuilder builder = new BasicProducerConfigBuilder();
    builder.setBootstrapServers("localhost:4242");
    builder.setCompressionType(CompressionType.none);
    builder.setAcks(Acks.all);
    Map<String, Object> p = builder.build();
    assertEquals("localhost:4242", p.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals(CompressionType.none.name(), p.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    assertEquals(Acks.all.name(), p.get(ProducerConfig.ACKS_CONFIG));
    assertEquals(StringSerializer.class.getName(), p.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals(AdaptrisMessageSerializer.class.getName(), p.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
  }

}
