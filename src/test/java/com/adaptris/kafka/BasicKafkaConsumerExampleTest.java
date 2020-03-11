package com.adaptris.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.core.ConfiguredConsumeDestination;
import com.adaptris.core.ConsumerCase;
import com.adaptris.core.StandaloneConsumer;

public class BasicKafkaConsumerExampleTest extends ConsumerCase {

  private static Logger log = LoggerFactory.getLogger(BasicKafkaConsumerExampleTest.class);


  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }

  @Override
  protected String createBaseFileName(Object object) {
    return ((StandaloneConsumer) object).getConsumer().getClass().getName() + "-BasicConsumerConfig";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    BasicConsumerConfigBuilder b = new BasicConsumerConfigBuilder("localhost:4242");
    PollingKafkaConsumer c = new PollingKafkaConsumer(new ConfiguredConsumeDestination("myTopic"), b);
    return new StandaloneConsumer(c);
  }

}
