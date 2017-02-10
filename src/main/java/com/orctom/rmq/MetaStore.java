package com.orctom.rmq;

import com.orctom.rmq.exception.RMQException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.orctom.rmq.Constants.PREFIX_QUEUE;
import static com.orctom.rmq.Constants.SUFFIX_OFFSET;

class MetaStore extends AbstractStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetaStore.class);

  private static final String NAME = "meta";

  private final Options options = new Options().setCreateIfMissing(true);
  private final RocksDB db;

  MetaStore(String id) {
    try {
      db = RocksDB.open(options, getPath(id, NAME));
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  List<String> getAllQueues() {
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

  void queueCreated(String queueName) {
    put(PREFIX_QUEUE + queueName, queueName);
  }

  void queueDeleted(String queueName) {
    delete(PREFIX_QUEUE + queueName);
  }

  String getOffset(String queueName) {
    return get(queueName + SUFFIX_OFFSET);
  }

  void setOffset(String queueName, String offset) {
    LOGGER.trace("[{}] new offset: {}", queueName, offset);
    put(queueName + SUFFIX_OFFSET, offset);
  }

  private void put(String key, String value) {
    put(key.getBytes(), value.getBytes());
  }

  private void put(byte[] key, byte[] value) {
    try {
      db.put(key, value);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  private String get(String key) {
    byte[] value = get(key.getBytes());
    if (null == value) {
      return null;
    }
    return new String(value);
  }

  private byte[] get(byte[] key) {
    try {
      return db.get(key);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  private void delete(String key) {
    delete(key.getBytes());
  }

  private void delete(byte[] key) {
    try {
      db.delete(key);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  @Override
  public void close() {
    LOGGER.debug("Closing MetaStore...");
    options.close();
    if (null != db) {
      db.close();
    }
    LOGGER.debug("Closed MetaStore...");
  }
}
