package com.adaptris.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import com.adaptris.core.CoreException;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class AdvancedConsumerConfigBuilderTest {

  @Test
  public void testConfig() {
    AdvancedConsumerConfigBuilder builder = new AdvancedConsumerConfigBuilder();
    assertNotNull(builder.getConfig());
    KeyValuePairSet myConfig = new KeyValuePairSet();
    myConfig.add(new KeyValuePair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:4242"));
    builder.setConfig(myConfig);
    assertEquals(myConfig, builder.getConfig());
    try {
      builder.setConfig(null);
      fail();
    } catch (IllegalArgumentException expected) {

    }
    assertEquals(myConfig, builder.getConfig());

  }

  @Test
  public void testBuild() throws Exception {
    KeyValuePairSet myConfig = new KeyValuePairSet();
    myConfig.add(new KeyValuePair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:4242"));
    myConfig.add(new KeyValuePair(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Password.encode("MyPassword", Password.PORTABLE_PASSWORD)));
    AdvancedConsumerConfigBuilder builder = new AdvancedConsumerConfigBuilder(myConfig);

    Map<String, Object> p = builder.build();
    assertEquals("localhost:4242", p.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("MyPassword", p.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG));

    assertEquals(StringDeserializer.class.getName(), p.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals(AdaptrisMessageDeserializer.class.getName(), p.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    assertEquals(StringSerializer.class.getName(), p.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals(AdaptrisMessageSerializer.class.getName(), p.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
  }

  @Test
  public void testBuild_WithDuffPassword() throws Exception {
    KeyValuePairSet myConfig = new KeyValuePairSet();
    myConfig.add(new KeyValuePair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:4242"));
    myConfig.add(new KeyValuePair(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "PW:ABCDEWF"));

    AdvancedConsumerConfigBuilder builder = new AdvancedConsumerConfigBuilder(myConfig);

    try {
      builder.build();
      fail();
    } catch (CoreException e) {
      assertNotNull(e.getCause());
      assertEquals(PasswordException.class, e.getCause().getClass());
    }
  }

}
