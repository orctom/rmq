package com.orctom.rmq;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class Queue implements Runnable, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Queue.class);

  private String name;
  private ColumnFamilyDescriptor descriptor;
  private ColumnFamilyHandle handle;

  private MetaStore metaStore;
  private QueueStore queueStore;

  private volatile boolean hasNoMoreMessage = true;
  private final Object lock = new Object();

  private List<RMQConsumer> consumers = new ArrayList<>();
  private int count = 0;

  Queue(String name,
        ColumnFamilyDescriptor descriptor,
        ColumnFamilyHandle handle,
        MetaStore metaStore,
        QueueStore queueStore) {
    this.name = name;
    this.descriptor = descriptor;
    this.handle = handle;

    this.metaStore = metaStore;
    this.queueStore = queueStore;
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

  Queue addConsumers(RMQConsumer... consumers) {
    return addConsumers(Lists.newArrayList(consumers));
  }

  Queue addConsumers(List<RMQConsumer> consumers) {
    this.consumers.addAll(consumers);
    this.consumers = this.consumers.stream().distinct().collect(Collectors.toList());
    signalNewConsumer();
    return this;
  }

  void removeConsumers(RMQConsumer... consumers) {
    this.consumers.removeAll(Lists.newArrayList(consumers));
  }

  @Override
  public void run() {
    LOGGER.trace("[{}] started.", name);
    while (!Thread.currentThread().isInterrupted()) {
      String offset = metaStore.getOffset(name);
      LOGGER.trace("[{}] loading from offset: {}", name, offset);
      try {
        if (isToWaitForConsumersAndMessages()) {
          synchronized (lock) {
            if (isToWaitForConsumersAndMessages()) {
              LOGGER.trace("[{}] waiting for consumers or new messages.", name);
              lock.wait();
              continue;
            }
          }
        }
        LOGGER.trace("[{}] loading", name);
        try (RocksIterator iterator = getPositionedIterator(offset)) {
          sendMessagesToConsumer(iterator);
        }
      } catch (InterruptedException ignored) {
        LOGGER.warn("[{}] Stopping...", name);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
    LOGGER.warn("[{}] Stopped.", name);
  }

  private RocksIterator getPositionedIterator(String offset) {
    RocksIterator iterator = queueStore.iter(this);
    if (Strings.isNullOrEmpty(offset)) {
      iterator.seekToFirst();
    } else {
      byte[] offsetBytes = offset.getBytes();
      iterator.seek(offsetBytes);
      if (Arrays.equals(offsetBytes, iterator.key())) {
        iterator.next();
      }
    }
    return iterator;
  }

  private void sendMessagesToConsumer(RocksIterator iterator) {
    int numberOfSentMessages = 0;
    for (; iterator.isValid(); iterator.next()) {
      String id = new String(iterator.key());
      byte[] msg = iterator.value();
      Message message = new Message(id, msg);
      try {
        Ack ack = sendToConsumer(message);
        switch (ack) {
          case HALT: {
            try {
              TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException ignored) {
            }
            return;
          }
          case LATER: {
            queueStore.pushToLater(name, message);
          }
          default: {
            metaStore.setOffset(name, id);
          }
        }
      } catch (Exception e) {
        queueStore.pushToLater(name, message);
        LOGGER.error(e.getMessage(), e);
      }
      numberOfSentMessages++;
    }
    if (0 == numberOfSentMessages) {
      hasNoMoreMessage = true;
    }
  }

  private Ack sendToConsumer(Message message) {
    if (hasNoConsumers()) {
      return Ack.HALT;
    }

    try {
      RMQConsumer consumer = getConsumer();
      if (null == consumer) {
        consumer = getConsumer();
      }
      return consumer.onMessage(message);
    } catch (IndexOutOfBoundsException ignored) {
      return Ack.HALT;
    }
  }

  private RMQConsumer getConsumer() {
    int index = getNextConsumerIndex();
    return consumers.get(index);
  }

  private int getNextConsumerIndex() {
    int size = consumers.size();
    if (1 == size) {
      return 0;
    }

    return Math.abs(count++) % size;
  }

  private boolean hasNoConsumers() {
    return 0 == consumers.size();
  }

  private void signalNewConsumer() {
    if (1 == consumers.size()) {
      signal();
    }
  }

  void signalNewMessage() {
    if (hasNoMoreMessage) {
      signal();
    }
  }

  private void signal() {
    hasNoMoreMessage = false;
    synchronized (lock) {
      lock.notify();
      LOGGER.trace("[{}] new messages or consumers signal", name);
    }
  }

  private boolean isToWaitForConsumersAndMessages() {
    return hasNoMoreMessage || consumers.isEmpty();
  }

  @Override
  public void close() throws Exception {
    handle.close();
    queueStore.close();
  }
}
