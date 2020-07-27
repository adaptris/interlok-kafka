package com.adaptris.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.BaseCase;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;

@SuppressWarnings("deprecation")
public class MockProducerTest {

  private static Logger log = LoggerFactory.getLogger(MockProducerTest.class);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {}

  @AfterClass
  public static void tearDownClass() {}

  @Test
  public void testProducerLifecycle_NoRecordKey() throws Exception {
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
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
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
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
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandardKafkaProducer producer = new StandardKafkaProducer() {
      @Override
      protected KafkaProducer<String, AdaptrisMessage> createProducer(Map<String, Object> config) {
        return kafkaProducer;
      }
    };
    producer.setRecordKey("hello");
    producer.setTopic(text);
    StandaloneProducer sp = new StandaloneProducer(
        new KafkaConnection(new SimpleConfigBuilder("localhost:9999")), producer);
    try {
      LifecycleHelper.initAndStart(sp);
    } finally {
      LifecycleHelper.stopAndClose(sp);
    }
  }

  @Test
  public void testProducerLifecycle_WithException_Legacy() throws Exception {
    final String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandardKafkaProducer producer =
        new StandardKafkaProducer(text, text, new BasicProducerConfigBuilder()) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(
              Map<String, Object> config) {
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
    final String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandaloneProducer producer =
        new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:5672")),
            new StandardKafkaProducer(text, text) {
              @Override
              protected KafkaProducer<String, AdaptrisMessage> createProducer(
                  Map<String, Object> config) {
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
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandardKafkaProducer producer =
        new StandardKafkaProducer(text, text, new BasicProducerConfigBuilder()) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(
              Map<String, Object> config) {
            return kafkaProducer;
          }
        };
    Mockito.when(kafkaProducer.send((ProducerRecord<String, AdaptrisMessage>) Mockito.any()))
        .thenThrow(new KafkaException(text));
    StandaloneProducer sp = new StandaloneProducer(producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    try {
      ServiceCase.execute(sp, msg);
      fail();
    } catch (ServiceException expected) {
      assertNotNull(expected.getCause());
      assertEquals(text, expected.getCause().getMessage());
    }
  }

  @Test
  public void testProduce_WithException() throws Exception {
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandaloneProducer producer =
        new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:1234")),
            new StandardKafkaProducer(text, text) {
              @Override
              protected KafkaProducer<String, AdaptrisMessage> createProducer(
                  Map<String, Object> config) {
                return kafkaProducer;
              }
            });
    Mockito.when(kafkaProducer.send((ProducerRecord<String, AdaptrisMessage>) Mockito.any()))
        .thenThrow(new KafkaException(text));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    try {
      ServiceCase.execute(producer, msg);
      fail();
    } catch (ServiceException expected) {
      assertNotNull(expected.getCause());
      assertEquals(text, expected.getCause().getMessage());
    }
  }

  @Test
  public void testProduce_Legacy() throws Exception {
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandardKafkaProducer producer =
        new StandardKafkaProducer(text, text, new BasicProducerConfigBuilder()) {
          @Override
          protected KafkaProducer<String, AdaptrisMessage> createProducer(
              Map<String, Object> config) {
            return kafkaProducer;
          }
        };
    StandaloneProducer sp = new StandaloneProducer(producer);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ServiceCase.execute(sp, msg);
  }

  @Test
  public void testProduce() throws Exception {
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandaloneProducer producer =
        new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:12345")),
            new StandardKafkaProducer(text, text) {
              @Override
              protected KafkaProducer<String, AdaptrisMessage> createProducer(
                  Map<String, Object> config) {
                return kafkaProducer;
              }
            });
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ServiceCase.execute(producer, msg);
  }

  @Test
  public void testProduce_PartitionedProducer_InvalidPartition() throws Exception {
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandaloneProducer producer =
        new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:12345")),
            new PartitionedKafkaProducer(text, text) {
              @Override
              protected KafkaProducer<String, AdaptrisMessage> createProducer(
                  Map<String, Object> config) {
                return kafkaProducer;
              }
            }.withPartition("XXX"));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ServiceCase.execute(producer, msg);
  }

  @Test
  public void testProduce_PartitionedProducer_WithPartition() throws Exception {
    String text = testName.getMethodName();
    final KafkaProducer<String, AdaptrisMessage> kafkaProducer = Mockito.mock(KafkaProducer.class);
    StandaloneProducer producer =
        new StandaloneProducer(new KafkaConnection(new SimpleConfigBuilder("localhost:12345")),
            new PartitionedKafkaProducer(text, text) {
              @Override
              protected KafkaProducer<String, AdaptrisMessage> createProducer(
                  Map<String, Object> config) {
                return kafkaProducer;
              }
            }.withPartition("0"));
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
    ServiceCase.execute(producer, msg);
  }
}
