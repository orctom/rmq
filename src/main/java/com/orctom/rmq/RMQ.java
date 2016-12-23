package com.orctom.rmq;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RMQ implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private static final RMQ INSTANCE = new RMQ();

  private QueueStore queueStore;

  private RMQ() {
    MetaStore metaStore = MetaStore.getInstance();
    List<String> queueNames = metaStore.getAllQueues();
    LOGGER.debug("init queues: {}", queueNames);
    if (queueNames.isEmpty()) {
      queueStore = new QueueStore(metaStore);
    } else {
      queueNames.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY));
      queueStore = new QueueStore(metaStore, queueNames);
    }
  }

  public static RMQ getInstance() {
    return INSTANCE;
  }

  public void send(String queueName, String message) {
    queueStore.push(queueName, message);
  }

  public void subscribe(String queueName, RMQConsumer... consumers) {
    queueStore.subscribe(queueName, consumers);
  }

  public void unsubscribe(String queueName, RMQConsumer... consumers) {
    queueStore.unsubscribe(queueName, consumers);
  }

  @Override
  public void close() throws Exception {
    queueStore.close();
  }
}
