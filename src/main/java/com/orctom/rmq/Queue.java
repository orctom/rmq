package com.orctom.rmq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class Queue {

  private String name;

  private Collection<RMQConsumer> consumers  = new ArrayList<>();

  private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(100_000);

  Queue(String name) {
    this.name = name;
  }

  String getName() {
    return name;
  }

  Collection<RMQConsumer> getConsumers() {
    return consumers;
  }

  void addConsumers(RMQConsumer... consumers) {
    Collections.addAll(this.consumers, consumers);
  }

  void addConsumers(Collection<RMQConsumer> consumers) {
    this.consumers.addAll(consumers);
  }

  void send(String message) {
    for (RMQConsumer consumer : consumers) {
      consumer.onMessage(message);
    }
  }
}
