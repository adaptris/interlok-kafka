package com.adaptris.kafka;

import com.adaptris.core.StandaloneProducer;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.kafka.ConfigBuilder.Acks;
import com.adaptris.kafka.ConfigBuilder.CompressionType;

public class BasicKafkaProducerExampleTest extends ExampleProducerCase {

  @Override
  protected String createBaseFileName(Object object) {
    return ((StandaloneProducer) object).getProducer().getClass().getName() + "-BasicProducerConfig";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {

    SimpleConfigBuilder b = new SimpleConfigBuilder("localhost:4242");
    b.setCompressionType(CompressionType.none);
    b.setAcks(Acks.all);
    StandardKafkaProducer producer = new StandardKafkaProducer("MyProducerRecordKey", "MyTopic");
    StandaloneProducer result = new StandaloneProducer(new KafkaConnection(b), producer);

    return result;
  }
}
