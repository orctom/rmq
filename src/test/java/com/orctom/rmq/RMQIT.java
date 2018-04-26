package com.orctom.rmq;

import com.orctom.laputa.utils.SimpleMeter;
import com.orctom.laputa.utils.SimpleMetrics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RMQIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQIT.class);

  @Test
  public void test() {
    String topic = "events";
    RMQOptions options = new RMQOptions("dummy", 1000, false);
    try (RMQ rmq = RMQ.getInstance(options)) {
      rmq.subscribe(topic, new DummyConsumer());
      SimpleMetrics metrics = SimpleMetrics.create(LOGGER, 5, TimeUnit.SECONDS);
      SimpleMeter meter = metrics.meter("sent");
      for (int i = 0; i < 500_000; i++) {
        String msg = String.valueOf(System.currentTimeMillis());
        rmq.push(topic, msg);
        meter.mark();
      }

      sleepFor(20, TimeUnit.SECONDS);
    }
    System.out.println("..............");
    sleepFor(20, TimeUnit.SECONDS);
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
