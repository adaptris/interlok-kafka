package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.BaseCase;

public class MockConsumerTest extends BaseTestClass {

  @Test
  public void testLoggingContext() {
    StandardKafkaConsumer consumer = new StandardKafkaConsumer();
    assertFalse(consumer.additionalDebug());
    assertNull(consumer.getAdditionalDebug());
    consumer.setAdditionalDebug(Boolean.FALSE);
    assertEquals(Boolean.FALSE, consumer.getAdditionalDebug());
    assertFalse(consumer.additionalDebug());
  }

  @Test
  public void testLifecycle() throws Exception {
    final String text = getName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = mockKafkaConsumer();
    ConsumerRecords<String, AdaptrisMessage> records = mock(ConsumerRecords.class);
    StandardKafkaConsumer consumer = new StandardKafkaConsumer() {
      @Override
      KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
        return kafkaConsumer;
      }
    };
    consumer.withTopics(text);
    consumer.setAdditionalDebug(true);

    when(records.count()).thenReturn(0);
    when(records.iterator()).thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>().iterator());
    when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);
    when(kafkaConsumer.poll(any())).thenReturn(records);
    StandaloneConsumer sc = new StandaloneConsumer(new KafkaConnection(new SimpleConfigBuilder("localhost:4242")), consumer);
    try {
      LifecycleHelper.initAndStart(sc);
      LifecycleHelper.stopAndClose(sc);
    } finally {
      LifecycleHelper.stopAndClose(sc);
    }
  }

  @Test
  public void testLifecycle_WithException() throws Exception {
    final String text = getName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = mockKafkaConsumer();
    ConsumerRecords<String, AdaptrisMessage> records = mock(ConsumerRecords.class);
    StandardKafkaConsumer consumer = new StandardKafkaConsumer() {
      @Override
      KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
        throw new RuntimeException(text);
      }
    };
    consumer.withTopics(text);
    when(records.count()).thenReturn(0);
    when(records.iterator()).thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>().iterator());
    when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);
    when(kafkaConsumer.poll(any())).thenReturn(records);
    StandaloneConsumer sc = new StandaloneConsumer(new KafkaConnection(new SimpleConfigBuilder("localhost:4242")), consumer);
    try {
      LifecycleHelper.initAndStart(sc);
      fail();
    } catch (CoreException e) {
      assertNotNull(e.getCause());
      assertEquals(text, e.getCause().getMessage());
      LifecycleHelper.stopAndClose(sc);
    }
  }

  @Test
  public void testConsume() throws Exception {
    final String text = getName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = mockKafkaConsumer();
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);

    ConsumerRecords<String, AdaptrisMessage> records = mock(ConsumerRecords.class);
    ConsumerRecord<String, AdaptrisMessage> record = new ConsumerRecord<>(text, 0, 0, text, msg);

    StandardKafkaConsumer consumer = new StandardKafkaConsumer() {
      @Override
      KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
        return kafkaConsumer;
      }
    };

    consumer.withTopics(text);
    when(records.count()).thenReturn(1);
    when(records.iterator()).thenReturn(new ArrayList<>(Arrays.asList(record)).iterator());
    when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);
    when(kafkaConsumer.poll(any())).thenReturn(records);
    StandaloneConsumer sc = new StandaloneConsumer(new KafkaConnection(new SimpleConfigBuilder("localhost:4242")), consumer);
    MockMessageListener mock = new MockMessageListener();
    sc.registerAdaptrisMessageListener(mock);
    try {
      LifecycleHelper.initAndStart(sc);

      BaseCase.waitForMessages(mock, 1);
      assertTrue(mock.getMessages().size() >= 1);
      AdaptrisMessage consumed = mock.getMessages().get(0);
      assertEquals(text, consumed.getContent());
    } finally {
      LifecycleHelper.stopAndClose(sc);
    }
  }

  private KafkaConsumer<String, AdaptrisMessage> mockKafkaConsumer() {
    return mock(KafkaConsumer.class);
  }

}
