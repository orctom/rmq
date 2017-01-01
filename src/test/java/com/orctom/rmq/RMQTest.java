package com.orctom.rmq;

import com.orctom.laputa.utils.MutableInt;
import com.orctom.laputa.utils.SimpleMetrics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RMQTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQTest.class);

  @Test
  public void test() {
    String topic = "events";
    RMQ.setTtl(1000);
    RMQ rmq = RMQ.getInstance();
    rmq.subscribe(topic, new DummyConsumer());
    SimpleMetrics metrics = SimpleMetrics.create(LOGGER, 5, TimeUnit.SECONDS);
    MutableInt counter = metrics.meter("sent");
    for (int i = 0; i < 10_000_000; i++) {
      rmq.send(topic, "" + System.currentTimeMillis());
      counter.increase();
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
