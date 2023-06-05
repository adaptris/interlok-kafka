package com.adaptris.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

public class BaseTestClass {

  public TestInfo testInfo;

  @BeforeEach
  public void setUp(TestInfo info) {
    testInfo = info;
  }

  protected String getName() {
    return testInfo.getDisplayName().substring(0, testInfo.getDisplayName().indexOf("("));
  }

}
