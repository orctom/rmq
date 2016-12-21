package com.orctom.rmq;

import com.orctom.rmq.exception.RMQException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class MetaStore extends AbstractStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetaStore.class);

  private static final MetaStore INSTANCE = new MetaStore();

  private static final String PREFIX_QUEUE = "queue_";
  private static final String SUFFIX_OFFSET = "_offset";

  private final Options options = new Options().setCreateIfMissing(true);
  private final RocksDB db;
  private QueueStore queueStore;

  private MetaStore() {
    ensureDataDirExist();
    try {
      db = RocksDB.open(options, getPath("meta"));
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }

    initQueueStore();
  }

  static MetaStore getInstance() {
    return INSTANCE;
  }

  QueueStore getQueueStore() {
    return queueStore;
  }

  private void initQueueStore() {
    List<String> queueNames = getAllQueues();
    if (queueNames.isEmpty()) {
      LOGGER.warn("No queue yet, will initialize it later");
      return;
    }
    queueStore = new QueueStore(queueNames, 30);
  }

  private List<String> getAllQueues() {
    try (final RocksIterator iterator = db.newIterator()) {
      List<String> names = new ArrayList<>();
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        String key = new String(iterator.key());
        if (key.startsWith(PREFIX_QUEUE)) {
          names.add(new String(iterator.value()));
        }
      }
      return names;
    }
  }

  String getOffset(String queueName) {
    return get(queueName + SUFFIX_OFFSET);
  }

  void setOffset(String queueName, String offset) {
    put(queueName + SUFFIX_OFFSET, offset);
  }

  private void put(String key, String value) {
    put(key, value);
  }

  private void put(byte[] key, byte[] value) {
    try {
      db.put(key, value);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  private String get(String key) {
    return new String(get(key.getBytes()));
  }

  private byte[] get(byte[] key) {
    try {
      return db.get(key);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    options.close();
    if (null != db) {
      db.close();
    }
  }
}
