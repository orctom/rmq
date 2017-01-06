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
    return "/home/chenhao/workspaces-hao/pipeline/.data/roleB/" + id;
//    return "data/" + id;
  }

  public static RocksDB read(String id) {
    try {
      String path = getPath(id);
      System.out.println(path);
      return RocksDB.open(options, path);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }

  public static void read(String id, List<String> queueNames) {
    try {
      List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
      for (String queueName : queueNames) {
        descriptors.add(new ColumnFamilyDescriptor(queueName.getBytes(), new ColumnFamilyOptions()));
      }
      queueNames.add("default");
      descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
      RocksDB db = RocksDB.open(dbOptions, getPath(id), descriptors, handles);
      int i = 0;
      for (ColumnFamilyHandle handle : handles) {
        System.out.println(queueNames.get(i));
        iterate(db.newIterator(handle));
        i++;
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
      RocksDB db = read("meta");
      RocksIterator iterator = db.newIterator();
      iterate(iterator);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void readCF() {
    try {
      read("queues", Lists.newArrayList("inbox", "inbox_later", "ready", "ready_later", "sent"));
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
//    RockStore.readSolo();
    RockStore.readCF();
  }
}
