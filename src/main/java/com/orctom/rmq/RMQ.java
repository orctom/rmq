package com.orctom.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

public class RMQ {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private static final RMQ INSTANCE = new RMQ();

  private QueueStore store;

  private Map<String, Queue> queues = new HashMap<>();

  private RMQ() {
    List<String> queueNames = readQueueNames();
    store = new QueueStore(queueNames, 1000);
  }

  public static RMQ getInstance() {
    return INSTANCE;
  }

  private List<String> readQueueNames() {
    return new ArrayList<>(MetaStore.getInstance().getAll().values());
  }

  public void createQueue(String queueName) {
    store.createQueue(queueName);
  }

  public void deleteQueue(String queueName) {
    store.deleteQueue(queueName);
  }

  public void send(String topic, String message) {
    getQueue(topic).send(message);
    store.push(topic, null, message);
  }

  public void subscribe(String topic, RMQConsumer... consumers) {
    if (null == consumers) {
      return;
    }

    Queue queue = getQueue(topic);
    queue.addConsumers(consumers);
  }

  public void subscribe(String topic, Collection<RMQConsumer> consumers) {
    if (null == consumers || consumers.isEmpty()) {
      return;
    }

    Queue queue = getQueue(topic);
    queue.addConsumers(consumers);
  }

  private Queue getQueue(String topic) {
    return queues.computeIfAbsent(topic, f -> new Queue(topic));
  }
}
