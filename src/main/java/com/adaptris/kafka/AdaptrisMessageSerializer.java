package com.adaptris.kafka;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.adaptris.core.AdaptrisMessage;

/**
 * Serializer implementation for {@link AdaptrisMessage} objects.
 * <p>
 * Note that this simply preserves the byte[] payload, and no metadata for interoperability with other consumers and producers.
 * </p>
 * 
 * @author lchan
 *
 */
public class AdaptrisMessageSerializer implements Serializer<AdaptrisMessage> {

  public AdaptrisMessageSerializer() {
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] serialize(String topic, AdaptrisMessage data) {
    return data.getPayload();
  }

  @Override
  public void close() {}

}
