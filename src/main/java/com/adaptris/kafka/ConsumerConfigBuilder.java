package com.adaptris.kafka;

import java.util.Map;

import com.adaptris.core.CoreException;
import com.adaptris.kafka.ConfigDefinition.FilterKeys;

public interface ConsumerConfigBuilder extends ConfigBuilder {

  @Override
  default Map<String, Object> build() throws CoreException {
    return build(FilterKeys.Consumer);
  }

}
