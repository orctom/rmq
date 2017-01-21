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
    RMQOptions options = new RMQOptions("dummy", 1000, false);
    try (RMQ rmq = RMQ.getInstance(options)) {
      rmq.subscribe(topic, new DummyConsumer());
      SimpleMetrics metrics = SimpleMetrics.create(LOGGER, 5, TimeUnit.SECONDS);
      MutableInt counter = metrics.meter("sent");
      for (int i = 0; i < 1_0; i++) {
        String msg = String.valueOf(System.currentTimeMillis());
        rmq.send(topic, msg);
        counter.increase();
      }

      sleepFor(10, TimeUnit.SECONDS);
    }
    System.out.println("..............");
    sleepFor(5, TimeUnit.SECONDS);
    System.out.println("terminate.");
  }

  private void sleepFor(long time, TimeUnit timeUnit) {
    try {
      timeUnit.sleep(time);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
