package com.adaptris.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.adaptris.kafka.ConfigBuilder.KeyFilter;

/**
 * Helper for building a filtered Kafka configuration.
 *
 */
public class ConfigDefinition {

  /**
   * Wraps the static methods as an enum.
   */
  public static enum FilterKeys implements KeyFilter {
    Producer(() -> {
      return producer().retainKeys();
    }),

    Consumer(() -> {
      return consumer().retainKeys();
    }),

    ProducerOrConsumer(() -> {
      return both().retainKeys();

    });

    private KeyFilter filter;

    FilterKeys(KeyFilter f) {
      filter = f;
    }

    @Override
    public Collection<String> retainKeys() {
      return filter.retainKeys();
    }
  }

  /**
   * {@code org.apache.kafka.clients.producer.ProducerConfig#configNames()} with {@code key.serializer} and {@code value.serializer}
   */
  public static KeyFilter producer() {
    return () -> {
      return Stream
          .concat(ProducerConfig.configNames().stream(),
              Arrays.asList(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).stream())
          .collect(Collectors.toSet());
    };
  }

  /**
   * {@code org.apache.kafka.clients.consumer.ConsumerConfig#configNames()} with {@code key.deserializer} and {@code value.deserializer}
   */
  public static KeyFilter consumer() {
    return () -> {
      return Stream
          .concat(ConsumerConfig.configNames().stream(),
              Arrays.asList(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).stream())
          .collect(Collectors.toSet());
    };
  }

  /**
   * Union of both {@link #producer()} and {@link #consumer()}.
   */
  public static KeyFilter both() {
    return () -> {
      return CollectionUtils.union(producer().retainKeys(), consumer().retainKeys());
    };
  }

}
