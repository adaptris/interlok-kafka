package com.adaptris.kafka;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.DefaultMessageFactory;

public class AdaptrisMessageDeserializerTest extends BaseTestClass {
  private static final String CHAR_ENC = "UTF-8";

  @Test
  public void testConfigure() {
    try (AdaptrisMessageDeserializer s = new AdaptrisMessageDeserializer()) {
      s.configure(new HashMap<String, Object>(), false);
      assertEquals(AdaptrisMessageFactory.getDefaultInstance(), s.messageFactory());
      s.configure(createConfig(CHAR_ENC), false);
      assertNotSame(AdaptrisMessageFactory.getDefaultInstance(), s.messageFactory());
    }
  }

  @Test
  public void testClose() {
    AdaptrisMessageSerializer s = new AdaptrisMessageSerializer();
    s.close();
  }

  @Test
  public void testDeserializer() throws Exception {
    try (AdaptrisMessageDeserializer s = new AdaptrisMessageDeserializer()) {
      s.configure(createConfig(CHAR_ENC), false);
      AdaptrisMessage m = s.deserialize(getName(), "Hello World".getBytes(CHAR_ENC));
      assertEquals("Hello World", m.getContent());
      assertEquals(getName(), m.getMessageHeaders().get(AdaptrisMessageDeserializer.KAFKA_TOPIC_KEY));
    }
  }

  private Map<String, Object> createConfig(String charEncoding) {
    Map<String, Object> config = new HashMap<>();
    DefaultMessageFactory f = new DefaultMessageFactory();
    if (!isEmpty(charEncoding)) {
      f.setDefaultCharEncoding(charEncoding);
    }
    config.put(ConfigBuilder.KEY_DESERIALIZER_FACTORY_CONFIG, f);
    return config;
  }

}
