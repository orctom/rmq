package com.orctom.rmq;

import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RMQTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQTest.class);

  @Test
  public void test() {
    String topic = "events";
    RMQ rmq = RMQ.getInstance();
    for (int i = 0; i < 5; i++) {
      rmq.send(topic, i + "-0-" + DateTime.now().toString("yyyy-MM-dd HH:mm:ss:SSSSSSS"));
    }
    rmq.subscribe(topic, new DummyConsumer());
    sleepFor(7, TimeUnit.SECONDS);
    for (int i = 0; i < 10; i++) {
      rmq.send(topic, i + "-1-" + DateTime.now().toString("yyyy-MM-dd HH:mm:ss:SSSSSSS"));
    }
    sleepFor(7, TimeUnit.SECONDS);
    for (int i = 0; i < 10; i++) {
      rmq.send(topic, i + "-2-" + DateTime.now().toString("yyyy-MM-dd HH:mm:ss:SSSSSSS"));
    }


    sleepFor(20, TimeUnit.MINUTES);
  }

  private void sleepFor(long time, TimeUnit timeUnit) {
    try {
      timeUnit.sleep(time);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
