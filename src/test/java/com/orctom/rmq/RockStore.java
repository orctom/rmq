package com.orctom.rmq;

import com.google.common.collect.Lists;
import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class RockStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(RockStore.class);

  private static Options options = new Options().setCreateIfMissing(true);

  private static String getPath(String id) {
    return "data/" + id;
  }

  public static RocksDB read(String id) {
    try {
      return RocksDB.open(options, getPath(id));
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  public static void read(String id, List<String> queueNames) {
    try {
      List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
      descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
      for (String queueName : queueNames) {
        descriptors.add(new ColumnFamilyDescriptor(queueName.getBytes(), new ColumnFamilyOptions()));
      }
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
      RocksDB db = RocksDB.open(dbOptions, getPath(id), descriptors, handles);
      for (ColumnFamilyHandle handle : handles) {
        System.out.println("handle: " + handle);
        iterate(db.newIterator(handle));
      }
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  static void ensureDataDirExist() {
    File dataDir = new File(".", "data");
    if (dataDir.exists()) {
      return;
    }
    boolean created = dataDir.mkdirs();
    LOGGER.trace("ensuring data dir existence, created: {}", created);
  }

  static void readSolo() {
    try {
      RocksDB db = read("queues");
      RocksIterator iterator = db.newIterator();
      iterate(iterator);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void readCF() {
    try {
      read("queues", Lists.newArrayList("events"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void iterate(RocksIterator iterator) {
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      String key = new String(iterator.key());
      String value = new String(iterator.value());
      System.out.println(key + " -> " + value);
    }
  }

  public static void main(String[] args) {
    RockStore.readSolo();
//    RockStore.readCF();
  }
}
