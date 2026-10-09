package com.adaptris.kafka;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.adaptris.core.CoreException;
import com.adaptris.kafka.ConfigDefinition.FilterKeys;

public class ProducerConfigBuilderTest {

  @Test
  public void testBuildUsesProducerFilter() throws Exception {
    Map<String, Object> config = Map.of("bootstrap.servers", "localhost:9092");
    ProducerConfigBuilder builder = filter -> {
      assertSame(FilterKeys.Producer, filter);
      return config;
    };

    assertSame(config, builder.build());
  }

  @Test
  public void testBuildPropagatesCoreException() {
    CoreException failure = new CoreException("Cannot build producer configuration");
    ProducerConfigBuilder builder = filter -> {
      assertSame(FilterKeys.Producer, filter);
      throw failure;
    };

    assertSame(failure, assertThrows(CoreException.class, builder::build));
  }
}
