package com.adaptris.kafka;

import com.adaptris.core.StandaloneConsumer;
import com.adaptris.interlok.junit.scaffolding.ExampleConsumerCase;

public class BasicKafkaConsumerExampleTest extends ExampleConsumerCase {

  @Override
  protected String createBaseFileName(Object object) {
    return ((StandaloneConsumer) object).getConsumer().getClass().getName() + "-BasicConsumerConfig";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    BasicConsumerConfigBuilder b = new BasicConsumerConfigBuilder("localhost:4242");
    PollingKafkaConsumer c = new PollingKafkaConsumer(b).withTopics("myTopic");
    return new StandaloneConsumer(c);
  }

}
