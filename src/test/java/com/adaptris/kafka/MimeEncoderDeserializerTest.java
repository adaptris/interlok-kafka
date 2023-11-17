package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.MimeEncoder;

public class MimeEncoderDeserializerTest extends BaseTestClass {

  @Test
  public void testDeserializer() throws Exception {
    try (MimeEncoderDeserializer s = new MimeEncoderDeserializer()) {
      Map<String, Object> config = new HashMap<>();
      config.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, new DefaultMessageFactory());
      s.configure(config, false);
      AdaptrisMessage base = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
      AdaptrisMessage m = s.deserialize(getName(), new MimeEncoder().encode(base));
      assertEquals("Hello World", m.getContent());
      assertEquals(base.getUniqueId(), m.getUniqueId());
      assertEquals(getName(), m.getMessageHeaders().get(AdaptrisMessageDeserializer.KAFKA_TOPIC_KEY));
    }
  }

  @Test
  public void testDeserializer_WithError() throws Exception {
    try (MimeEncoderDeserializer s = new MimeEncoderDeserializer()) {
      Map<String, Object> config = new HashMap<>();
      config.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, new DefaultMessageFactory());
      s.configure(config, false);
      assertThrows(RuntimeException.class, () -> s.deserialize(getName(), "Hello World".getBytes()));
    }
  }

}
