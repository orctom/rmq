package com.orctom.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyConsumer implements RMQConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DummyConsumer.class);

  @Override
  public Ack onMessage(String msg) {
    LOGGER.info("received: {}", msg);
    return Ack.DONE;
  }
}
