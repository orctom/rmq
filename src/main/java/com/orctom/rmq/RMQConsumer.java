package com.orctom.rmq;

public interface RMQConsumer {

  Ack onMessage(String message);
}
