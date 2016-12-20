package com.orctom.rmq;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;

public class Queue implements AutoCloseable {

  private String name;
  private ColumnFamilyDescriptor descriptor;
  private ColumnFamilyHandle handle;

  private Collection<RMQConsumer> consumers  = new ArrayList<>();

  private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(100_000);

  public static Queue create(String name) {
    return new Queue(name);
  }

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

  void send(String message) {
    for (RMQConsumer consumer : consumers) {
      consumer.onMessage(message);
    }
  }

  public ColumnFamilyDescriptor getDescriptor() {
    return descriptor;
  }

  public ColumnFamilyHandle getHandle() {
    return handle;
  }

  public ArrayBlockingQueue<String> getQueue() {
    return queue;
  }

  @Override
  public void close() throws Exception {

  }
}
