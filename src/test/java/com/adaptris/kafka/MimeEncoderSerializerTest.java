package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;

public class MimeEncoderSerializerTest {

  @Test
  public void testSerializer() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    try (MimeEncoderSerializer s = new MimeEncoderSerializer()) {
      s.configure(Collections.emptyMap(), false);
      assertNotNull(s.serialize("", msg));
    }
  }

  @Test
  public void testSerializer_WithException() throws Exception {
    try (MimeEncoderSerializer s = new MimeEncoderSerializer()) {
      s.configure(Collections.emptyMap(), false);
      assertThrows(RuntimeException.class, () -> s.serialize("", null));
    }
  }

}
