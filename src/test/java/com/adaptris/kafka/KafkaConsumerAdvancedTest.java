package com.adaptris.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.adaptris.core.StandaloneConsumer;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class KafkaConsumerAdvancedTest extends KafkaConsumerSimpleTest {

  @Override
  protected String createBaseFileName(Object object) {
    return ((StandaloneConsumer) object).getConsumer().getClass().getName() + "-AdvancedConfigBuilder";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    KeyValuePairSet myConfig = new KeyValuePairSet();
    myConfig.add(new KeyValuePair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:4242"));
    myConfig.add(new KeyValuePair(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000"));
    myConfig.add(new KeyValuePair(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "62000"));
    myConfig.add(new KeyValuePair(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"));
    AdvancedConfigBuilder adv = new AdvancedConfigBuilder(myConfig);
    StandardKafkaConsumer c = new StandardKafkaConsumer().withTopics("myTopic");
    return new StandaloneConsumer(new KafkaConnection(adv), c);
  }

}
