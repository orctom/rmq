package com.orctom.rmq;

import com.google.common.collect.Lists;
import com.orctom.laputa.utils.MutableLong;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

class Queue implements Runnable, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Queue.class);

  private String name;
  private ColumnFamilyDescriptor descriptor;
  private ColumnFamilyHandle handle;

  private MetaStore metaStore;
  private QueueStore queueStore;

  private volatile boolean hasNoMoreMessage = true;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition noConsumerOrMessage = lock.newCondition();

  private final ReentrantLock consumersLock = new ReentrantLock();
  private volatile List<RMQConsumer> consumers = new ArrayList<>();
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

  public void setHandle(ColumnFamilyHandle handle) {
    this.handle = handle;
  }

  List<RMQConsumer> getConsumers() {
    return consumers;
  }

  Queue addConsumers(RMQConsumer... consumers) {
    return addConsumers(Lists.newArrayList(consumers));
  }

  private Queue addConsumers(List<RMQConsumer> consumers) {
    try {
      consumersLock.lock();
      this.consumers.addAll(consumers);
      this.consumers = this.consumers.stream().distinct().collect(Collectors.toList());
      signalNewConsumer();
      return this;
    } finally {
      consumersLock.unlock();
    }
  }

  void removeConsumers(RMQConsumer... consumers) {
    this.consumers.removeAll(Lists.newArrayList(consumers));
  }

  @Override
  public void run() {
    LOGGER.trace("[{}] started.", name);
    while (!Thread.currentThread().isInterrupted()) {
      LOGGER.trace("[{}] loading...", name);
      try {
        if (isToWaitForConsumersAndMessages()) {
          try {
            lock.lock();
            if (isToWaitForConsumersAndMessages()) {
              LOGGER.trace("[{}] waiting for consumers or new messages.", name);
              noConsumerOrMessage.await();
              continue;
            }
          } finally {
            lock.unlock();
          }
        }

        LOGGER.trace("[{}] sending", name);
        try (RocksIterator iterator = queueStore.positionedIterator(this)) {
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

  private void sendMessagesToConsumer(RocksIterator iterator) {
    int numberOfSentMessages = 0;
    for (; iterator.isValid(); iterator.next()) {
      String id = new String(iterator.key());
      byte[] msg = iterator.value();
      Message message = new Message(id, msg);
      try {
        Ack ack = sendToConsumer(message);
        switch (ack) {
          case WAIT: {
            try {
              TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException ignored) {
            }
            return;
          }
          case LATER: {
            LOGGER.trace("Message: {} marked as LATER.", id);
            return;
          }
          case DONE:{
            LOGGER.trace("Message: {} marked as DONE.", id);
            metaStore.setOffset(name, id);
            return;
          }
          default: {
            LOGGER.trace("Message: {} marked as LATER.", id);
            return;
          }
        }
      } catch (Exception e) {
        LOGGER.info("Message: {} marked as LATER.", id);
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
      return Ack.WAIT;
    }

    try {
      RMQConsumer consumer = getConsumer();
      if (null == consumer) {
        consumer = getConsumer();
      }
      return consumer.onMessage(message);
    } catch (IndexOutOfBoundsException ignored) {
      return Ack.WAIT;
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
      LOGGER.trace("[{}] signaled first consumer.", name);
    }
  }

  void signalNewMessage() {
    if (hasNoMoreMessage) {
      signal();
      LOGGER.trace("[{}] signaled new messages.", name);
    }
  }

  private void signal() {
    hasNoMoreMessage = false;
    try {
      lock.lock();
      noConsumerOrMessage.signal();
    } finally {
      lock.unlock();
    }
  }

  private boolean isToWaitForConsumersAndMessages() {
    return hasNoMoreMessage || consumers.isEmpty();
  }

  @Override
  public void close() throws Exception {
    handle.close();
  }
}
