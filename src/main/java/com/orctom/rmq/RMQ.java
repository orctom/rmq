package com.orctom.rmq;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RMQ implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("rmq-meta-%d").build()
  );

  private static final Map<String, RMQ> INSTANCES = new ConcurrentHashMap<>();

  private RMQOptions options;
  private QueueStore queueStore;

  private RMQ(RMQOptions options) {
    this.options = options;

    MetaStore metaStore = new MetaStore(options.getId());
    List<String> queueNames = metaStore.getAllQueues();
    LOGGER.debug("init queues: {}", queueNames);
    if (queueNames.isEmpty()) {
      queueStore = new QueueStore(metaStore, options);
    } else {
      queueNames.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY));
      queueStore = new QueueStore(metaStore, queueNames, options);
    }
    scheduleMetaUpdater();
  }

  public static RMQ getInstance() {
    return getInstance(new RMQOptions());
  }

  public static RMQ getInstance(RMQOptions options) {
    return INSTANCES.computeIfAbsent(options.getId(), id -> new RMQ(options));
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

  public long getSize(String queueName) {
    return queueStore.getSize(queueName);
  }

  private void scheduleMetaUpdater() {
    LOGGER.debug("Starting cleaner");
    scheduler.scheduleWithFixedDelay(
        () -> queueStore.updateMeta(),
        0,
        15,
        TimeUnit.SECONDS
    );
  }

  @Override
  public void close() {
    LOGGER.debug("Closing...");
    shutdownScheduler();
    queueStore.close();

    INSTANCES.remove(options.getId());
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
