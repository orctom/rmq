package com.orctom.rmq;

import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RMQ implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private static final RMQ INSTANCE = new RMQ();

  private MetaStore metaStore;

  private Map<String, Queue> queues = new HashMap<>();

  private RMQ() {
    metaStore = MetaStore.getInstance();
  }

  public static RMQ getInstance() {
    return INSTANCE;
  }

  public Queue createQueue(String queueName) {
    return metaStore.getQueueStore().createQueue(queueName);
  }

  public void deleteQueue(String queueName) {
    Queue queue = queues.remove(queueName);
    metaStore.getQueueStore().deleteQueue(queue);
  }

  public void send(String queueName, String message) {
    metaStore.getQueueStore().push(getQueue(queueName), message);
  }

  public void subscribe(String queueName, RMQConsumer... consumers) {
    if (null == consumers) {
      return;
    }

    Queue queue = getQueue(queueName);
    queue.addConsumers(consumers);
  }

  public void subscribe(String queueName, Collection<RMQConsumer> consumers) {
    if (null == consumers || consumers.isEmpty()) {
      return;
    }

    Queue queue = getQueue(queueName);
    queue.addConsumers(consumers);
  }

  private Queue getQueue(String name) {
    return queues.computeIfAbsent(name, f -> new Queue(name));
  }

  MetaStore getMetaStore() {
    return metaStore;
  }

  Map<String, Queue> getQueues() {
    return queues;
  }

  @Override
  public void close() throws Exception {
    queues.values().forEach(queue -> {
      queue.getHandle().close();
    });
  }
}
