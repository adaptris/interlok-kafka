package com.adaptris.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;

@SuppressWarnings("deprecation")
public class MockConsumerTest {

  private static Logger log = LoggerFactory.getLogger(MockConsumerTest.class);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() {
  }

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
    final String text = testName.getMethodName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    ConsumerRecords<String, AdaptrisMessage> records = Mockito.mock(ConsumerRecords.class);
    StandardKafkaConsumer consumer =
        new StandardKafkaConsumer(new ConfiguredConsumeDestination(text)) {
          @Override
          KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
            return kafkaConsumer;
          }
        };
    Mockito.when(records.count()).thenReturn(0);
    Mockito.when(records.iterator()).thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>().iterator());
    Mockito.when(kafkaConsumer.poll(Mockito.anyLong())).thenReturn(records);
    Mockito.when(kafkaConsumer.poll(Mockito.anyObject())).thenReturn(records);
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
    final String text = testName.getMethodName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    ConsumerRecords<String, AdaptrisMessage> records = Mockito.mock(ConsumerRecords.class);
    StandardKafkaConsumer consumer =
        new StandardKafkaConsumer(new ConfiguredConsumeDestination(text)) {
          @Override
          KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
            throw new RuntimeException(text);
          }
        };
    Mockito.when(records.count()).thenReturn(0);
    Mockito.when(records.iterator()).thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>().iterator());
    Mockito.when(kafkaConsumer.poll(Mockito.anyLong())).thenReturn(records);
    Mockito.when(kafkaConsumer.poll(Mockito.anyObject())).thenReturn(records);
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
    final String text = testName.getMethodName();
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);

    ConsumerRecords<String, AdaptrisMessage> records = Mockito.mock(ConsumerRecords.class);
    ConsumerRecord<String, AdaptrisMessage> record = new ConsumerRecord<String, AdaptrisMessage>(text, 0, 0, text, msg);

    StandardKafkaConsumer consumer = new StandardKafkaConsumer(new ConfiguredConsumeDestination(text)) {
          @Override
          KafkaConsumer<String, AdaptrisMessage> createConsumer(Map<String, Object> config) {
            return kafkaConsumer;
          }
        };
        
    Mockito.when(records.count()).thenReturn(1);
    Mockito.when(records.iterator())
        .thenReturn(new ArrayList<ConsumerRecord<String, AdaptrisMessage>>(Arrays.asList(record)).iterator());
    Mockito.when(kafkaConsumer.poll(Mockito.anyLong())).thenReturn(records);
    Mockito.when(kafkaConsumer.poll(Mockito.anyObject())).thenReturn(records);
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


}
