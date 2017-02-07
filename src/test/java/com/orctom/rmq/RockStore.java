package com.orctom.rmq;

import com.google.common.base.Strings;
import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public abstract class RockStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(RockStore.class);

  private static Options options = new Options().setCreateIfMissing(true);

  private static void read(String path) {
    try {
      List<String> columnFamilies = readMeta(path + "/meta/");
      readQueues(path + "/queues/", columnFamilies);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private static List<String> readMeta(String path) throws RocksDBException {
    RocksDB db = RocksDB.open(options, path);
    RocksIterator iterator = db.newIterator();
    List<String> columnFamilies = new ArrayList<>();
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      String key = new String(iterator.key());
      String value = new String(iterator.value());
      LOGGER.info("{} -> {}", key, value);
      if (key.startsWith("queue_")) {
        columnFamilies.add(value);
      }
    }
    return columnFamilies;
  }

  private static void readQueues(String path, List<String> queueNames) {
    try {
      List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
      for (String queueName : queueNames) {
        descriptors.add(new ColumnFamilyDescriptor(queueName.getBytes(), new ColumnFamilyOptions()));
      }
      queueNames.add("default");
      descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
      RocksDB db = RocksDB.open(dbOptions, path, descriptors, handles);
      int i = 0;
      for (ColumnFamilyHandle handle : handles) {
        LOGGER.info("{}: ", queueNames.get(i));
        LOGGER.info("left: {}", iterate(db.newIterator(handle)));
        LOGGER.info("");
        i++;
      }
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  private static int iterate(RocksIterator iterator) {
    int count = 0;
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      String key = new String(iterator.key());
      String value = new String(iterator.value());
      LOGGER.info("{} -> {}", key, value);
      count ++;
    }
    return count;
  }


  public static void main(String[] args) {
    String folder = args[0];
    if (Strings.isNullOrEmpty(folder)) {
      System.out.println("Expecting one arg of abstract folder path of the .data directory");
      return;
    }
    Path path = Paths.get(folder);
    try {
      Files.newDirectoryStream(path).forEach(f -> {
        String dbPath = f.toFile().getAbsolutePath();
        LOGGER.info(dbPath);
        LOGGER.info("===================================");
        RockStore.read(dbPath);
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
