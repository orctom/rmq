package com.orctom.rmq;

import com.google.common.primitives.Longs;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class Queue implements Runnable, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Queue.class);

  private String name;
  private ColumnFamilyDescriptor descriptor;
  private ColumnFamilyHandle handle;

  private Collection<RMQConsumer> consumers  = new ArrayList<>();

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

  Collection<RMQConsumer> getConsumers() {
    return consumers;
  }

  void addConsumers(RMQConsumer... consumers) {
    Collections.addAll(this.consumers, consumers);
  }

  void addConsumers(Collection<RMQConsumer> consumers) {
    this.consumers.addAll(consumers);
  }

  @Override
  public void run() {
    MetaStore metaStore = RMQ.getInstance().getMetaStore();
    QueueStore queueStore = metaStore.getQueueStore();
    long offset = metaStore.getOffset(name);
    while (!Thread.currentThread().isInterrupted()) {
      try {
        RocksIterator iterator = queueStore.iter(this);
        for (iterator.seek(Longs.toByteArray(offset)); iterator.isValid(); iterator.next()) {
          for (RMQConsumer consumer : consumers) {
            consumer.onMessage(new String(iterator.value()));
          }
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

  ColumnFamilyDescriptor getDescriptor() {
    return descriptor;
  }

  ColumnFamilyHandle getHandle() {
    return handle;
  }

  @Override
  public void close() throws Exception {
    handle.close();
  }
}
