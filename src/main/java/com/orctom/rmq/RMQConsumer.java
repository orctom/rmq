package com.orctom.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RMQConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQConsumer.class);

  abstract void onMessage(String message);
}
