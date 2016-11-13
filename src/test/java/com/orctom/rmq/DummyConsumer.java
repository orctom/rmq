package com.orctom.rmq;

public class DummyConsumer extends RMQConsumer {

  @Override
  void onMessage(String msg) {
    System.out.println(msg);
  }
}
