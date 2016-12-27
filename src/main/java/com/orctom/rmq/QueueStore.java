package com.orctom.rmq;

import com.google.common.base.Strings;
import com.orctom.laputa.utils.IdGenerator;
import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.orctom.rmq.Constants.SUFFIX_LATER;

class QueueStore extends AbstractStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueStore.class);

  private static final String ID = "queues";

  private final MetaStore metaStore;
  private final RocksDB db;
  private final Options options = new Options().setCreateIfMissing(true);
  private final WriteOptions writeOptions = new WriteOptions();

  private final IdGenerator idGenerator = IdGenerator.create();

  private Map<String, Queue> queues = new HashMap<>();
  private Map<String, Thread> queueThreads = new HashMap<>();

  // ============================= constructors ============================

  QueueStore(MetaStore metaStore) {
    this.metaStore = metaStore;
    ensureDataDirExist();
    try {
      db = RocksDB.open(options, getPath(ID));
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  QueueStore(MetaStore metaStore, List<String> queueNames) {
    this.metaStore = metaStore;
    if (null == queueNames) {
      throw new IllegalArgumentException("QueueNames should not be null");
    }
    ensureDataDirExist();
    List<ColumnFamilyDescriptor> descriptors = createColumnFamilyDescriptors(queueNames);
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    try {
      db = RocksDB.open(getPath(ID), descriptors, handles);
      initQueues(queueNames, descriptors, handles);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  // ============================= queue apis ============================

  private Queue createQueue(String queueName) {
    try {
      ColumnFamilyDescriptor descriptor = createColumnFamilyDescriptor(queueName);
      ColumnFamilyHandle handle = db.createColumnFamily(descriptor);
      Queue queue = new Queue(queueName, descriptor, handle, metaStore, this);

      queues.put(queueName, queue);
      metaStore.queueCreated(queueName);

      startQueue(queue);
      return queue;
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  void deleteQueue(String queueName) {
    dropColumnFamily(queues.get(queueName).getHandle());
    queues.remove(queueName);
    metaStore.queueDeleted(queueName);
  }

  private Queue getQueue(String name) {
    return queues.computeIfAbsent(name, f -> createQueue(name));
  }

  private Queue getLaterQueue(String name) {
    String laterQueueName = name + SUFFIX_LATER;
    Queue laterQueue = queues.computeIfAbsent(laterQueueName, f -> createQueue(laterQueueName));
    laterQueue.addConsumers(getQueue(name).getConsumers());
    return laterQueue;
  }

  void subscribe(String queueName, RMQConsumer... consumers) {
    if (null == consumers) {
      return;
    }

    Queue queue = getQueue(queueName);
    queue.addConsumers(consumers);

    if (isNotLaterQueue(queueName)) {
      Queue laterQueue = getQueue(queueName + SUFFIX_LATER);
      laterQueue.addConsumers(consumers);
    }
  }

  void unsubscribe(String queueName, RMQConsumer... consumers) {
    if (null == consumers) {
      return;
    }

    Queue queue = getQueue(queueName);
    queue.removeConsumers(consumers);

    unsubscribeFromLaterQueue(queueName, consumers);
  }

  private void unsubscribeFromLaterQueue(String queueName, RMQConsumer[] consumers) {
    if (isNotLaterQueue(queueName)) {
      Queue laterQueue = queues.get(queueName + SUFFIX_LATER);
      if (null != laterQueue) {
        laterQueue.removeConsumers(consumers);
      }
    }
  }

  private void startQueue(Queue queue) {
    Thread thread = new Thread(queue);
    thread.start();
    queueThreads.put(queue.getName(), thread);
  }

  private void stopQueue(Queue queue) {
    Thread thread = queueThreads.get(queue.getName());
    thread.interrupt();
  }

  void push(String queueName, String message) {
    push(queueName, message, false);
  }

  void push(String queueName, Message message) {
    push(queueName, message, false);
  }

  void pushToLater(String queueName, Message message) {
    push(queueName, message, true);
  }

  private void push(String queueName, String message, boolean isLater) {
    Queue queue = isLater ? getLaterQueue(queueName) : getQueue(queueName);
    String id = String.valueOf(idGenerator.generate());
    LOGGER.trace("[{}] new message, {}: {}", queueName, id, message);
    push(queue, id, message);
  }

  private void push(String queueName, Message message, boolean isLater) {
    Queue queue = isLater ? getLaterQueue(queueName) : getQueue(queueName);
    LOGGER.trace("[{}] new message, {}", queueName, message);
    push(queue, message);
  }

  void delete(String queueName, String id) {
    delete(getQueue(queueName), id);
  }

  private boolean isLaterQueue(String queueName) {
    return queueName.endsWith(SUFFIX_LATER);
  }

  private boolean isNotLaterQueue(String queueName) {
    return !isLaterQueue(queueName);
  }

  // ============================= low level apis ============================

  RocksIterator iter(Queue queue) {
    return db.newIterator(queue.getHandle());
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

  private void initQueues(List<String> queueNames,
                          List<ColumnFamilyDescriptor> descriptors,
                          List<ColumnFamilyHandle> handles) {
    for (int i = 0; i < handles.size(); i++) {
      String queueName = queueNames.get(i);
      ColumnFamilyDescriptor descriptor = descriptors.get(i);
      ColumnFamilyHandle handle = handles.get(i);
      Queue queue = new Queue(queueName, descriptor, handle, metaStore, this);
      queues.put(queueName, queue);
      startQueue(queue);
    }
  }

  private void push(Queue queue, String key, String value) {
    push(queue, key.getBytes(), value.getBytes());
  }

  private void push(Queue queue, Message message) {
    push(queue, message.getId().getBytes(), message.getValue().getBytes());
  }

  private void push(Queue queue, byte[] key, byte[] value) {
    try {
      db.put(queue.getHandle(), key, value);
      queue.signalNewMessage();
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  private void delete(Queue queue, String key) {
    delete(queue, key.getBytes());
  }

  private void delete(Queue queue, byte[] key) {
    try {
      db.delete(queue.getHandle(), writeOptions, key);
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

  void move(String key, Queue sourceQueue, Queue targetQueue) {
    move(key.getBytes(), sourceQueue, targetQueue);
  }

  void move(byte[] key, Queue sourceQueue, Queue targetQueue) {
    byte[] value = get(sourceQueue, key);
    delete(sourceQueue, key);
    push(targetQueue, key, value);
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

  void cleanDeletedMessages() {
    LOGGER.debug("Cleaning deleted messages");
    for (Queue queue : queues.values()) {
      String queueName = queue.getName();
      String offset = metaStore.getOffset(queueName);
      try {
        if (Strings.isNullOrEmpty(offset)) {
          continue;
        }
        Long start = System.currentTimeMillis();
        clean(queue, queueName, Long.valueOf(offset));
        long end = System.currentTimeMillis();
        LOGGER.debug("Done, took: {} ms", (end - start));
      } catch (NumberFormatException e) {
        LOGGER.error("[{}] wrong offset: {}", queueName, offset);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

  private void clean(Queue queue, String queueName, long offset) {
    LOGGER.trace("[{}] cleaning messages before offset: {} ", queueName, offset);
    RocksIterator iterator = db.newIterator(queue.getHandle());
    WriteBatch batch = new WriteBatch();
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      byte[] key = iterator.key();
      if (Long.valueOf(new String(key)) >= offset) {
        break;
      }
      batch.remove(queue.getHandle(), iterator.key());
    }
    try {
      if (batch.count() > 0) {
        db.write(writeOptions, batch);
      }
    } catch (RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    queues.values().forEach(this::stopQueue);
    metaStore.close();
    options.close();
    if (null != db) {
      db.close();
    }
  }
}
