package com.orctom.rmq;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.orctom.rmq.RMQOptions.DEFAULT_ID;
import static com.orctom.rmq.RMQOptions.DEFAULT_TTL;

public class RMQ implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private static final Map<String, RMQ> INSTANCES = new HashMap<>();

  private final RMQOptions options;

  private QueueStore queueStore;

  private RMQ(String id, int ttl, boolean batchMode) {
    options = new RMQOptions(id, ttl, batchMode);

    MetaStore metaStore = new MetaStore(id);
    List<String> queueNames = metaStore.getAllQueues();
    LOGGER.debug("init queues: {}", queueNames);
    if (queueNames.isEmpty()) {
      queueStore = new QueueStore(metaStore, id, ttl, batchMode);
    } else {
      queueNames.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY));
      queueStore = new QueueStore(metaStore, queueNames, id, ttl, batchMode);
    }
    startCleaner();
  }

  public static RMQ getInstance() {
    return INSTANCES.computeIfAbsent(DEFAULT_ID, id -> new RMQ(id, DEFAULT_TTL, false));
  }

  public static RMQ getInstance(RMQOptions options) {
    return INSTANCES.computeIfAbsent(options.getId(), id -> new RMQ(id, options.getTtl(), options.isBatchMode()));
  }

  public void send(String queueName, String message) {
    queueStore.push(queueName, message);
  }

  public void send(String queueName, Message message) {
    queueStore.push(queueName, message);
  }

  public void delete(String queueName, String id) {
    queueStore.delete(queueName, id);
  }

  public void subscribe(String queueName, RMQConsumer... consumers) {
    LOGGER.debug("[{}] subscribed by {}.", queueName, consumers);
    queueStore.subscribe(queueName, consumers);
  }

  public void unsubscribe(String queueName, RMQConsumer... consumers) {
    LOGGER.debug("[{}] unsubscribe by {}.", queueName, consumers);
    queueStore.unsubscribe(queueName, consumers);
  }

  private void startCleaner() {
    LOGGER.debug("Starting cleaner");
    scheduler.scheduleWithFixedDelay(
        () -> queueStore.cleanDeletedMessages(),
        5,
        15,
        TimeUnit.SECONDS
    );
  }

  @Override
  public void close() {
    LOGGER.debug("Closing...");
    shutdownScheduler();
    queueStore.close();
    LOGGER.debug("Closed.");
  }

  private void shutdownScheduler() {
    LOGGER.debug("Shutting down scheduler...");
    scheduler.shutdown();
    try {
      scheduler.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
    }
    scheduler.shutdownNow();
    LOGGER.debug("Scheduler stopped.");
  }
}
