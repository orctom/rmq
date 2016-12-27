package com.orctom.rmq;

import com.orctom.laputa.utils.SimpleMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DummyConsumer implements RMQConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DummyConsumer.class);

  private SimpleMetrics metrics = SimpleMetrics.create(LOGGER, 5, TimeUnit.SECONDS);

  @Override
  public Ack onMessage(Message msg) {
    metrics.mark("got");
//    LOGGER.info("received: {}", msg);
    return Ack.DONE;
  }
}
