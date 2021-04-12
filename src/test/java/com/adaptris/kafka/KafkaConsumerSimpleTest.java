package com.adaptris.kafka;

import com.adaptris.core.StandaloneConsumer;
import com.adaptris.interlok.junit.scaffolding.ExampleConsumerCase;

public class KafkaConsumerSimpleTest extends ExampleConsumerCase {

  @Override
  protected String createBaseFileName(Object object) {
    return ((StandaloneConsumer) object).getConsumer().getClass().getName() + "-SimpleConfigBuilder";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    StandardKafkaConsumer c = new StandardKafkaConsumer().withTopics("myTopic");
    return new StandaloneConsumer(new KafkaConnection(new SimpleConfigBuilder("localhost:4242")), c);
  }

}
