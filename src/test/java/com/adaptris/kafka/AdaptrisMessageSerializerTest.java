package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.HashMap;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.DefaultMessageFactory;

public class AdaptrisMessageSerializerTest {

  @Test
  public void testConfigure() {
    try (AdaptrisMessageSerializer s = new AdaptrisMessageSerializer()) {
      s.configure(new HashMap<String, Object>(), false);
    }
  }

  @Test
  public void testClose() {
    AdaptrisMessageSerializer s = new AdaptrisMessageSerializer();
    s.close();
  }

  @Test
  public void testSerializer() throws Exception {
    AdaptrisMessage undefined = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    AdaptrisMessage empty = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    DefaultMessageFactory utf8_fac = new DefaultMessageFactory();
    utf8_fac.setDefaultCharEncoding("UTF-8");
    AdaptrisMessage utf8 = utf8_fac.newMessage("Hello World");
    try (AdaptrisMessageSerializer s = new AdaptrisMessageSerializer()) {
      assertArrayEquals("Hello World".getBytes(), s.serialize("", undefined));
      assertArrayEquals("Hello World".getBytes("UTF-8"), s.serialize("", utf8));
      assertArrayEquals(new byte[0], s.serialize("", empty));
    }
  }

}
