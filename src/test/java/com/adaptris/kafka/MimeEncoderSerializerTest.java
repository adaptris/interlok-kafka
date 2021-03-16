package com.adaptris.kafka;

import static org.junit.Assert.assertNotNull;

import java.util.Collections;

import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;

public class MimeEncoderSerializerTest {

  @Test
  public void testSerializer() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    MimeEncoderSerializer s = new MimeEncoderSerializer();
    s.configure(Collections.emptyMap(), false);
    assertNotNull(s.serialize("", msg));
  }

  @Test(expected = RuntimeException.class)
  public void testSerializer_WithException() throws Exception {
    MimeEncoderSerializer s = new MimeEncoderSerializer();
    s.configure(Collections.emptyMap(), false);
    s.serialize("", null);
  }

}
