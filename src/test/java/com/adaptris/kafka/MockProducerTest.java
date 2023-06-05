package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;

public class MockProducerTest extends BaseTestClass {

  @Test
  public void testProducerLifecycle_NoRecordKey() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandardKafkaProducer producer = new StandardKafkaProducer() {
      @Override
      protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
        return kafkaProducer;
      }
    };
    producer.setTopic(text);
    try {
      LifecycleHelper.initAndStart(producer);
      fail();
    } catch (CoreException e) {

    } finally {
      LifecycleHelper.stopAndClose(producer);
    }
  }

  @Test
  public void testProducerLifecycle_Legacy() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandardKafkaProducer producer = new StandardKafkaProducer() {
      @Override
      protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
        return kafkaProducer;
      }
    };
    producer.setRecordKey("hello");
    producer.setTopic(text);
    producer.setProducerConfig(new BasicProducerConfigBuilder("localhost:1234"));
    try {
      LifecycleHelper.initAndStart(producer);
    } finally {
      LifecycleHelper.stopAndClose(producer);
    }
  }

  @Test
  public void testProducerLifecycle() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandardKafkaProducer producer = new StandardKafkaProducer() {
      @Override
      protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
        return kafkaProducer;
      }
    };
    producer.setRecordKey("hello");
    producer.setTopic(text);
    StandaloneProducer sp = new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:9999")), producer);
    try {
      LifecycleHelper.initAndStart(sp);
    } finally {
      LifecycleHelper.stopAndClose(sp);
    }
  }

  @Test
  public void testProducerLifecycle_WithException_Legacy() throws Exception {
    final String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandardKafkaProducer producer = new StandardKafkaProducer(text, text, new BasicProducerConfigBuilder()) {
      @Override
      protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
        throw new RuntimeException(text);
      }
    };
    try {
      LifecycleHelper.init(producer);
      LifecycleHelper.start(producer);
      fail();
    } catch (CoreException e) {
      assertNotNull(e.getCause());
      assertEquals(text, e.getCause().getMessage());
      BaseCase.stop(producer);
    }
  }

  @Test
  public void testProducerLifecycle_WithException() throws Exception {
    final String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandaloneProducer producer = new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:5672")),
        new StandardKafkaProducer(text, text) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
            throw new RuntimeException(text);
          }
        });
    try {
      LifecycleHelper.init(producer);
      LifecycleHelper.start(producer);
      fail();
    } catch (CoreException e) {
      assertNotNull(e.getCause());
      assertEquals(text, e.getCause().getMessage());
      BaseCase.stop(producer);
    }
  }

  @Test
  public void testProduce_WithException_Legacy() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandardKafkaProducer producer = new StandardKafkaProducer(text, text, new BasicProducerConfigBuilder()) {
      @Override
      protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
        return kafkaProducer;
      }
    };
    when(kafkaProducer.send(any(ProducerRecord.class))).thenThrow(new KafkaException(text));
    StandaloneProducer sp = new StandaloneProducer(producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    try {
      ExampleServiceCase.execute(sp, msg);
      fail();
    } catch (ServiceException expected) {
      assertNotNull(expected.getCause());
      assertEquals(text, expected.getCause().getMessage());
    }
  }

  @Test
  public void testProduce_WithException() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandaloneProducer producer = new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:1234")),
        new StandardKafkaProducer(text, text) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
            return kafkaProducer;
          }
        });
    when(kafkaProducer.send(any(ProducerRecord.class))).thenThrow(new KafkaException(text));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    try {
      ExampleServiceCase.execute(producer, msg);
      fail();
    } catch (ServiceException expected) {
      assertNotNull(expected.getCause());
      assertEquals(text, expected.getCause().getMessage());
    }
  }

  @Test
  public void testProduce_Legacy() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandardKafkaProducer producer = new StandardKafkaProducer(text, text, new BasicProducerConfigBuilder()) {
      @Override
      protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
        return kafkaProducer;
      }
    };
    StandaloneProducer sp = new StandaloneProducer(producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ExampleServiceCase.execute(sp, msg);
  }

  @Test
  public void testProduce() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandaloneProducer producer = new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:12345")),
        new StandardKafkaProducer(text, text) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
            return kafkaProducer;
          }
        });
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ExampleServiceCase.execute(producer, msg);
  }

  @Test
  public void testProduce_PartitionedProducer_InvalidPartition() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandaloneProducer producer = new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:12345")),
        new PartitionedKafkaProducer(text, text) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
            return kafkaProducer;
          }
        }.withPartition("XXX"));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ExampleServiceCase.execute(producer, msg);
  }

  @Test
  public void testProduce_PartitionedProducer_WithPartition() throws Exception {
    String text = getName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = mock(KafkaProducer.class);
    StandaloneProducer producer = new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:12345")),
        new PartitionedKafkaProducer(text, text) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
            return kafkaProducer;
          }
        }.withPartition("0"));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ExampleServiceCase.execute(producer, msg);
  }
}
