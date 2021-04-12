package com.adaptris.kafka;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.MimeEncoder;

public class MimeEncoderDeserializerTest {
  @Rule
  public TestName testName = new TestName();


  @Test
  public void testDeserializer() throws Exception {
    MimeEncoderDeserializer s = new MimeEncoderDeserializer();
    Map<String, Object> config = new HashMap<>();
    config.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, new DefaultMessageFactory());
    s.configure(config, false);
    AdaptrisMessage base = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    AdaptrisMessage m = s.deserialize(testName.getMethodName(), new MimeEncoder().encode(base));
    assertEquals("Hello World", m.getContent());
    assertEquals(base.getUniqueId(), m.getUniqueId());
    assertEquals(testName.getMethodName(), m.getMessageHeaders().get(AdaptrisMessageDeserializer.KAFKA_TOPIC_KEY));
  }

  @Test(expected = RuntimeException.class)
  public void testDeserializer_WithError() throws Exception {
    MimeEncoderDeserializer s = new MimeEncoderDeserializer();
    Map<String, Object> config = new HashMap<>();
    config.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, new DefaultMessageFactory());
    s.configure(config, false);
    s.deserialize(testName.getMethodName(), "Hello World".getBytes());
  }

}
