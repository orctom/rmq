package com.orctom.rmq;

import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;

import java.util.HashMap;
import java.util.Map;

class MetaStore extends AbstractStore {

  private static final MetaStore INSTANCE = new MetaStore();

  private final Options options = new Options().setCreateIfMissing(true);
  private final RocksDB db;

  private MetaStore() {
    ensureDataDirExist();
    try {
      db = RocksDB.open(options, getPath("meta"));
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  static MetaStore getInstance() {
    return INSTANCE;
  }

  void put(String key, String value) {
    put(key, value);
  }

  void put(byte[] key, byte[] value) {
    try {
      db.put(key, value);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  String get(String key) {
    return new String(get(key.getBytes()));
  }

  byte[] get(byte[] key) {
    try {
      return db.get(key);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  void write(WriteBatch writeBatch) {
    try (final WriteOptions writeOptions = new WriteOptions()) {
      db.write(writeOptions, writeBatch);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  Map<String, String> getAll() {
    try (final RocksIterator iterator = db.newIterator()) {
      Map<String, String> all = new HashMap<>();
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        all.put(new String(iterator.key()), new String(iterator.value()));
      }
      return all;
    }
  }

  void close() {
    options.close();
    if (null != db) {
      db.close();
    }
  }
}
