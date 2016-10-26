package com.orctom.rmq;

import com.google.common.base.Stopwatch;
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

  private void persist(WriteBatch batch) {
    int size = batch.count();
    if (0 == size) {
      LOGGER.debug("batch size: 0, skipped.");
      return;
    }

    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      db.write(writeOptions, batch);
      stopwatch.stop();
      LOGGER.debug("batch size: {}, {}", size, stopwatch);
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

  void put(String queueName, String key, String value) {
    put(queueName, key.getBytes(), value.getBytes());
  }

  void put(String queueName, byte[] key, byte[] value) {
    writeBatches.get(queueName).put(key, value);
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

  void moveToQueue(byte[] key, String targetQueue) {
    
  }

  void close() {
    columnFamilyHandles.values().forEach(ColumnFamilyHandle::close);
    options.close();
    if (null != db) {
      db.close();
    }
  }
}
