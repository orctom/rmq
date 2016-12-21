package com.orctom.rmq;

import com.google.common.primitives.Longs;
import com.orctom.laputa.utils.IdGenerator;
import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

class QueueStore extends AbstractStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueStore.class);

  private static final String ID = "queues";

  private final TtlDB db;
  private final DBOptions options = new DBOptions();

  private final IdGenerator idGenerator = IdGenerator.create();

  QueueStore(List<String> queueNames, int ttl) {
    if (null == queueNames) {
      throw new IllegalArgumentException("QueueNames should not be null");
    }
    ensureDataDirExist();
    List<ColumnFamilyDescriptor> descriptors = createColumnFamilyDescriptors(queueNames);
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<Integer> ttlList = createTTLs(queueNames.size(), ttl);
    try {
      db = TtlDB.open(options, getPath(ID), descriptors, handles, ttlList, false);
      initQueues(queueNames, descriptors, handles);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  private List<ColumnFamilyDescriptor> createColumnFamilyDescriptors(List<String> families) {
    if (families.isEmpty()) {
      return Collections.emptyList();
    }

    return families.stream().map(this::createColumnFamilyDescriptor).collect(Collectors.toList());
  }

  private ColumnFamilyDescriptor createColumnFamilyDescriptor(String name) {
    return new ColumnFamilyDescriptor(name.getBytes(), createColumnFamilyOptions());
  }

  private ColumnFamilyOptions createColumnFamilyOptions() {
    return new ColumnFamilyOptions().setTableFormatConfig(
        new BlockBasedTableConfig().setFilter(new BloomFilter())
    );
  }

  private List<Integer> createTTLs(int size, int ttl) {
    if (0 == size) {
      return Collections.emptyList();
    }

    List<Integer> ttlList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      ttlList.add(ttl);
    }
    return ttlList;
  }

  private void initQueues(List<String> queueNames,
                          List<ColumnFamilyDescriptor> descriptors,
                          List<ColumnFamilyHandle> handles) {
    Map<String, Queue> queues = RMQ.getInstance().getQueues();
    for (int i = 0; i < handles.size(); i++) {
      String queueName = queueNames.get(i);
      ColumnFamilyDescriptor descriptor = descriptors.get(i);
      ColumnFamilyHandle handle = handles.get(i);
      Queue queue = new Queue(queueName, descriptor, handle);
      queues.put(queueName, queue);
    }
  }

  Queue createQueue(String queueName) {
    try {
      ColumnFamilyDescriptor descriptor = createColumnFamilyDescriptor(queueName);
      ColumnFamilyHandle handle = db.createColumnFamily(descriptor);
      Queue queue = new Queue(queueName, descriptor, handle);
      RMQ.getInstance().getQueues().put(queueName, queue);
      return queue;
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  void deleteQueue(Queue queue) {
    dropColumnFamily(queue.getHandle());
  }

  private void dropColumnFamily(ColumnFamilyHandle handle) {
    if (null == handle) {
      return;
    }

    try {
      db.dropColumnFamily(handle);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  void push(Queue queue, String message) {
    push(queue, String.valueOf(idGenerator.generate()), message);
  }

  private void push(Queue queue, String key, String value) {
    push(queue, key.getBytes(), value.getBytes());
  }

  private void push(Queue queue, byte[] key, byte[] value) {
    try {
      db.put(queue.getHandle(), key, value);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  RocksIterator iter(Queue queue) {
    return db.newIterator(queue.getHandle());
  }

  void remove(Queue queue, String key) {
    remove(queue, key.getBytes());
  }

  void remove(Queue queue, byte[] key) {
    try {
      db.remove(queue.getHandle(), key);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  String get(Queue queue, String key) {
    return new String(get(queue, key.getBytes()));
  }

  byte[] get(Queue queue, byte[] key) {
    try {
      return db.get(queue.getHandle(), key);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  String popString(Queue queue) {
    byte[] value = pop(queue);
    if (null == value) {
      return null;
    }

    return new String(value);
  }

  byte[] pop(Queue queue) {
    ColumnFamilyHandle handle = queue.getHandle();
    RocksIterator iterator = db.newIterator(handle);

    iterator.seekToLast();
    if (!iterator.isValid()) {
      return null;
    }

    byte[] key = iterator.key();
    byte[] value = iterator.value();
    try {
      db.remove(handle, key);
      return value;
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  void move(String key, Queue sourceQueue, Queue targetQueue) {
    move(key.getBytes(), sourceQueue, targetQueue);
  }

  void move(byte[] key, Queue sourceQueue, Queue targetQueue) {
    byte[] value = get(sourceQueue, key);
    remove(sourceQueue, key);
    push(targetQueue, key, value);
  }

  @Override
  public void close() {
    options.close();
    if (null != db) {
      db.close();
    }
  }
}
