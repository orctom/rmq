package com.orctom.rmq;

import com.google.common.collect.Lists;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Queue implements Runnable, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Queue.class);

  private String name;
  private ColumnFamilyDescriptor descriptor;
  private ColumnFamilyHandle handle;

  private boolean hasNoMoreMessage = true;
  private final Object lock = new Object();

  private List<RMQConsumer> consumers  = new ArrayList<>();

  Queue(String name) {
    this.name = name;
  }

  Queue(String name, ColumnFamilyDescriptor descriptor, ColumnFamilyHandle handle) {
    this.name = name;
    this.descriptor = descriptor;
    this.handle = handle;
  }

  String getName() {
    return name;
  }

  ColumnFamilyDescriptor getDescriptor() {
    return descriptor;
  }

  ColumnFamilyHandle getHandle() {
    return handle;
  }

  List<RMQConsumer> getConsumers() {
    return consumers;
  }

  void addConsumers(RMQConsumer... consumers) {
    addConsumers(Lists.newArrayList(consumers));
  }

  void addConsumers(Collection<RMQConsumer> consumers) {
    synchronized (lock) {
      this.consumers.addAll(consumers);
      lock.notify();
    }
  }

  void removeConsumers(RMQConsumer... consumers) {
    removeConsumers(Lists.newArrayList(consumers));
  }

  void removeConsumers(Collection<RMQConsumer> consumers) {
    this.consumers.removeAll(consumers);
  }

  @Override
  public void run() {
    MetaStore metaStore = RMQ.getInstance().getMetaStore();
    QueueStore queueStore = metaStore.getQueueStore();

    String offset = metaStore.getOffset(name);

    while (!Thread.currentThread().isInterrupted()) {
      try {
        while (consumers.isEmpty() || hasNoMoreMessage) {
          lock.wait();
        }
        RocksIterator iterator = queueStore.iter(this);
        for (iterator.seek(offset.getBytes()); iterator.isValid(); iterator.next()) {
          String newOffset = new String(iterator.key());
          for (RMQConsumer consumer : consumers) {
            consumer.onMessage(new String(iterator.value()));
          }
          metaStore.setOffset(name, newOffset);
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

  void signal() {
    hasNoMoreMessage = false;
    synchronized (lock) {
      lock.notify();
    }
  }

  @Override
  public void close() throws Exception {
    handle.close();
  }
}
