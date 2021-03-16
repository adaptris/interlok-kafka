package com.adaptris.kafka;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.DefaultMessageFactory;

public class AdaptrisMessageDeserializerTest {
  private static final String CHAR_ENC = "UTF-8";

  @Rule
  public TestName testName = new TestName();

  @Test
  public void testConfigure() {
    AdaptrisMessageDeserializer s = new AdaptrisMessageDeserializer();
    s.configure(new HashMap<String, Object>(), false);
    assertEquals(AdaptrisMessageFactory.getDefaultInstance(), s.messageFactory());
    s.configure(createConfig(CHAR_ENC), false);
    assertNotSame(AdaptrisMessageFactory.getDefaultInstance(), s.messageFactory());
  }

  @Test
  public void testClose() {
    AdaptrisMessageSerializer s = new AdaptrisMessageSerializer();
    s.close();
  }

  @Test
  public void testDeserializer() throws Exception {
    AdaptrisMessageDeserializer s = new AdaptrisMessageDeserializer();
    s.configure(createConfig(CHAR_ENC), false);
    AdaptrisMessage m = s.deserialize(testName.getMethodName(), "Hello World".getBytes(CHAR_ENC));
    assertEquals("Hello World", m.getContent());
    assertEquals(testName.getMethodName(), m.getMessageHeaders().get(AdaptrisMessageDeserializer.KAFKA_TOPIC_KEY));
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
