package com.orctom.rmq;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RMQTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQTest.class);

  @Test
  public void test() {
    DummyConsumer consumer = new DummyConsumer();
    String topic = "events";
    RMQ rmq = RMQ.getInstance();
    rmq.subscribe(topic, consumer);
    for (int i = 0; i < 5; i++) {
      rmq.send(topic, "msg: " + i);
    }
  }
}
