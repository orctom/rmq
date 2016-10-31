package com.orctom.rmq;

import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

class
QueueStore extends AbstractStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueStore.class);

  public static final int PERSIST_DELAY = 0;
  public static final int PERSIST_PERIOD = 1000;

  private static final String ID = "queue";

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  private final TtlDB db;
  private final DBOptions options = new DBOptions();
  private final WriteOptions writeOptions = new WriteOptions();

  private Map<String, WriteBatch> writeBatches = new ConcurrentHashMap<>();
  private Map<String, ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();

  QueueStore(List<String> queueNames, int ttl) {
    if (null == queueNames) {
      throw new IllegalArgumentException("QueueNames should not be null");
    }
    ensureDataDirExist();
    List<ColumnFamilyDescriptor> columnFamilyDescriptors = createColumnFamilyDescriptors(queueNames);
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<Integer> ttlList = createTTLs(queueNames.size(), ttl);
    try {
      db = TtlDB.open(options, getPath(ID), columnFamilyDescriptors, handles, ttlList, false);
      initHandlerMap(queueNames, handles);
      initWriteBatches();
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

  private void initHandlerMap(List<String> queueNames, List<ColumnFamilyHandle> handles) {
    for (int i = 0; i < handles.size(); i++) {
      String queueName = queueNames.get(i);
      ColumnFamilyHandle handle = handles.get(i);
      columnFamilyHandles.put(queueName, handle);
    }
  }

  private void initWriteBatches() {
    for (String queueName : columnFamilyHandles.keySet()) {
      writeBatches.put(queueName, new WriteBatch());
    }
    initWriteBatchThread();
  }

  private void initWriteBatchThread() {
    Timer timer = new Timer(false);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        LOGGER.info("Writing batch...");
        writeLock.lock();
        try {
          for (String queueName : columnFamilyHandles.keySet()) {
            WriteBatch writeBatch = writeBatches.put(queueName, new WriteBatch());
            persist(writeBatch);
          }
        } finally {
          writeLock.unlock();
        }
      }
    }, PERSIST_DELAY, PERSIST_PERIOD);
  }

  void persist(WriteBatch batch) {
    int size = batch.count();
    if (0 == size) {
      LOGGER.debug("batch size: 0, skipped.");
      return;
    }

    try {
      db.write(writeOptions, batch);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  void createQueue(String queueName) {
    try {
      ColumnFamilyHandle handle = db.createColumnFamily(createColumnFamilyDescriptor(queueName));
      columnFamilyHandles.put(queueName, handle);
      writeBatches.put(queueName, new WriteBatch());
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  void deleteQueue(String queueName) {
    dropColumnFamily(getColumnFamilyHandle(queueName));
    columnFamilyHandles.remove(queueName);
    writeBatches.remove(queueName);
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

  private ColumnFamilyHandle getColumnFamilyHandle(String queueName) {
    return columnFamilyHandles.get(queueName);
  }

  void push(String queueName, String key, String value) {
    push(queueName, key.getBytes(), value.getBytes());
  }

  void push(String queueName, byte[] key, byte[] value) {
    writeBatches.get(queueName).put(getColumnFamilyHandle(queueName), key, value);
  }

  void remove(String queueName, String key) {
    remove(queueName, key.getBytes());
  }

  void remove(String queueName, byte[] key) {
    readLock.lock();
    try {
      writeBatches.get(queueName).remove(getColumnFamilyHandle(queueName), key);
    } finally {
      readLock.unlock();
    }
  }

  String get(String queueName, String key) {
    return new String(get(queueName, key.getBytes()));
  }

  byte[] get(String queueName, byte[] key) {
    try {
      return db.get(getColumnFamilyHandle(queueName), key);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  String popString(String queueName) {
    byte[] value = pop(queueName);
    if (null == value) {
      return null;
    }

    return new String(value);
  }

  byte[] pop(String queueName) {
    ColumnFamilyHandle handle = getColumnFamilyHandle(queueName);
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

  void move(String key, String sourceQueue, String targetQueue) {
    move(key.getBytes(), sourceQueue, targetQueue);
  }

  void move(byte[] key, String sourceQueue, String targetQueue) {
    byte[] value = get(sourceQueue, key);
    remove(sourceQueue, key);
    push(targetQueue, key, value);
  }

  void close() {
    columnFamilyHandles.values().forEach(ColumnFamilyHandle::close);
    options.close();
    if (null != db) {
      db.close();
    }
  }
}
