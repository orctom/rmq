package com.orctom.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RMQ {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private static final RMQ INSTANCE = new RMQ();

  private QueueStore store;

  private RMQ() {
    List<String> queueNames = readQueueNames();
    store = new QueueStore(queueNames, 1000);
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

  public void push(String queueName, String data) {
    store.push(queueName, null, data);
  }

}
