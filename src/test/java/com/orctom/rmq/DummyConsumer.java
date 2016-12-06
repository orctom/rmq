package com.orctom.rmq;

public class DummyConsumer extends RMQConsumer {

  @Override
  Ack onMessage(String msg) {
    System.out.println(msg);
    return Ack.DONE;
  }
}
