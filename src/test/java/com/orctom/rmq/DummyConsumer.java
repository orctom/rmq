package com.orctom.rmq;

public class DummyConsumer implements RMQConsumer {

  @Override
  public Ack onMessage(String msg) {
    System.out.println(msg);
    return Ack.DONE;
  }
}
