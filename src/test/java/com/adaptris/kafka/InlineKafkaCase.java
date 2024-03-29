package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ClosedState;
import com.adaptris.core.CoreException;
import com.adaptris.core.InitialisedState;
import com.adaptris.core.StandaloneConsumer;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.StartedState;
import com.adaptris.core.StoppedState;
import com.adaptris.core.stubs.MockMessageListener;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.BaseCase;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.kafka.ConfigBuilder.Acks;
import com.adaptris.util.KeyValuePair;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;

public class InlineKafkaCase extends BaseTestClass {

  @RegisterExtension
  public static final SharedKafkaTestResource INLINE_KAFKA = new SharedKafkaTestResource().withBrokerProperty("auto.create.topics.enable",
      "false");

  @Test
  public void testStart_BadConfig() throws Exception {
    String text = getName();
    // No BootstrapServer, so we're duff.
    AdvancedConfigBuilder builder = new AdvancedConfigBuilder();
    StandaloneProducer p = new StandaloneProducer(new KafkaConnection(builder), new StandardKafkaProducer(text, text));
    try {
      LifecycleHelper.init(p);
      try {
        LifecycleHelper.start(p);
        fail();
      } catch (CoreException expected) {

      }
    } finally {
      LifecycleHelper.stopAndClose(p);
    }
  }

  @Test
  public void testProducerLifecycle() throws Exception {
    String text = getName();
    StandaloneProducer p = createProducer(INLINE_KAFKA.getKafkaConnectString(), text, text);
    try {
      LifecycleHelper.initAndStart(p);
      LifecycleHelper.stopAndClose(p);
    } finally {
      LifecycleHelper.stopAndClose(p);
    }
  }

  @Test
  public void testConsumerLifecycle() throws Exception {
    String topicName = getName();
    MockMessageListener mock = new MockMessageListener();
    StandaloneConsumer sc = createConsumer(INLINE_KAFKA.getKafkaConnectString(), topicName, mock);
    createTopic(topicName);
    try {
      LifecycleHelper.init(sc);
      assertEquals(InitialisedState.getInstance(), sc.retrieveComponentState());
      LifecycleHelper.start(sc);
      assertEquals(StartedState.getInstance(), sc.retrieveComponentState());
      Thread.sleep(1000);
      LifecycleHelper.stop(sc);
      assertEquals(StoppedState.getInstance(), sc.retrieveComponentState());
      LifecycleHelper.close(sc);
      assertEquals(ClosedState.getInstance(), sc.retrieveComponentState());
    } finally {
      BaseCase.stop(sc);
    }
  }

  // Test doesn't appear to work, since messages aren't being delivered :(
  // @Test
  public void testSendAndReceive_Polling() throws Exception {
    StandaloneConsumer sc = null;
    StandaloneProducer sp = null;
    try {
      String text = getName();
      createTopic(text);
      sp = createProducer(INLINE_KAFKA.getKafkaConnectString(), text, text);
      MockMessageListener mock = new MockMessageListener();
      sc = createConsumer(INLINE_KAFKA.getKafkaConnectString(), text, mock);
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(text);
      BaseCase.start(sc);
      ExampleServiceCase.execute(sp, msg);
      BaseCase.waitForMessages(mock, 1);
      assertEquals(1, mock.getMessages().size());
      AdaptrisMessage consumed = mock.getMessages().get(0);
      assertEquals(text, consumed.getContent());
    } finally {
      BaseCase.stop(sc, sp);
    }
  }

  private StandaloneConsumer createConsumer(String bootstrapServer, String topic, MockMessageListener p) {
    AdvancedConsumerConfigBuilder builder = new AdvancedConsumerConfigBuilder();
    builder.getConfig().add(new KeyValuePair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
    builder.getConfig().add(new KeyValuePair(ConsumerConfig.GROUP_ID_CONFIG, "group"));
    StandaloneConsumer sc = new StandaloneConsumer(new KafkaConnection(builder), createConsumer(bootstrapServer, topic));
    sc.registerAdaptrisMessageListener(p);
    return sc;
  }

  private StandaloneProducer createProducer(String bootstrapServer, String recordKey, String topic) {
    AdvancedConfigBuilder builder = new AdvancedConfigBuilder();
    builder.getConfig().add(new KeyValuePair(ProducerConfig.ACKS_CONFIG, Acks.none.actualValue()));
    builder.getConfig().add(new KeyValuePair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
    // Change MAX_BLOCK to stop each test from taking ~60000 which is the default block...
    // Can't figure out why KafkaProducer is why it is atm.
    builder.getConfig().add(new KeyValuePair(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100"));

    return new StandaloneProducer(new KafkaConnection(builder), createProducer(recordKey, topic));
  }

  private StandardKafkaConsumer createConsumer(String bootstrapServer, String topic) {
    AdvancedConsumerConfigBuilder builder = new AdvancedConsumerConfigBuilder();
    builder.getConfig().add(new KeyValuePair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
    builder.getConfig().add(new KeyValuePair(ConsumerConfig.GROUP_ID_CONFIG, "group"));
    StandardKafkaConsumer result = new StandardKafkaConsumer().withTopics(topic);
    return result;
  }

  private PartitionedKafkaProducer createProducer(String recordKey, String topic) {
    PartitionedKafkaProducer result = new PartitionedKafkaProducer();
    result.setRecordKey(recordKey);
    result.setTopic(topic);
    result.setPartition("1");
    return result;
  }

  private void createTopic(String name) {
    INLINE_KAFKA.getKafkaTestUtils().createTopic(name, 1, (short) -1);
    Set<String> topics = INLINE_KAFKA.getKafkaTestUtils().getTopicNames();
    assertTrue(topics.contains(name));
  }

}
