package com.adaptris.kafka;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;

public class LoggingContextTest {

  private Logger log = LoggerFactory.getLogger(this.getClass());

  @Test
  public void testLogPartitions() {
    final KafkaConsumer<String, AdaptrisMessage> kafkaConsumer = mock(KafkaConsumer.class);
    List<PartitionInfo> partInfo = Arrays.asList(new PartitionInfo("topic", 1, null, new Node[0], new Node[0]));
    Mockito.when(kafkaConsumer.partitionsFor(anyString())).thenReturn(partInfo);
    LoggingContext.LOGGER.logPartitions(new LoggingContextImpl(false), Arrays.asList("hello"), kafkaConsumer);
    LoggingContext.LOGGER.logPartitions(new LoggingContextImpl(true), Arrays.asList("hello"), kafkaConsumer);
  }

  private class LoggingContextImpl implements LoggingContext {
    private boolean debug;

    LoggingContextImpl(boolean additionalDebug) {
      debug = additionalDebug;
    }

    @Override
    public boolean additionalDebug() {
      return debug;
    }

    @Override
    public Logger logger() {
      return log;
    }
  }

}
