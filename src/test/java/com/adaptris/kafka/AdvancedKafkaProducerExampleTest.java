package com.adaptris.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;

import com.adaptris.core.StandaloneProducer;
import com.adaptris.interlok.junit.scaffolding.ExampleProducerCase;
import com.adaptris.kafka.ConfigBuilder.Acks;
import com.adaptris.kafka.ConfigBuilder.CompressionType;
import com.adaptris.util.KeyValuePair;

public class AdvancedKafkaProducerExampleTest extends ExampleProducerCase {

  @Override
  protected String createBaseFileName(Object object) {
    return ((StandaloneProducer) object).getProducer().getClass().getName() + "-AdvancedProducerConfig";
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {

    AdvancedConfigBuilder b = new AdvancedConfigBuilder();
    b.getConfig().add(new KeyValuePair(ProducerConfig.ACKS_CONFIG, Acks.all.name()));
    b.getConfig().add(new KeyValuePair(ProducerConfig.LINGER_MS_CONFIG, "10"));
    b.getConfig().add(new KeyValuePair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:4242"));
    b.getConfig().add(new KeyValuePair(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.lz4.name()));
    StandardKafkaProducer producer =
        new StandardKafkaProducer("MyProducerRecordKey", "MyTopic");
    StandaloneProducer result = new StandaloneProducer(new KafkaConnection(b), producer);

    return result;
  }

}
