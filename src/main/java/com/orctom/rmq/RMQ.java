package com.orctom.rmq;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RMQ {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private TtlDB db;
  private DBOptions options = new DBOptions();
  private WriteOptions writeOptions = new WriteOptions();

  public RMQ(String id) {
    this(id, 1800);
  }

  public RMQ(String id, int ttl) {
    String path = "data/" + id;
    ensureDataDirExist();
    open(path, ttl);
    initBatches();
    initBatchThread();
  }

  private void ensureDataDirExist() {
    File dataDir = new File(".", "data");
    if (dataDir.exists()) {
      return;
    }
    boolean created = dataDir.mkdirs();
    LOGGER.trace("ensuring data dir existence, created: {}", created);
  }

  private void open(String path, int ttl) {
    try {
      List<ColumnFamilyHandle> handles = new ArrayList<>();
      List<Integer> ttlList = Lists.newArrayList(ttl, ttl, ttl);
      db = TtlDB.open(options, path, COLUMN_FAMILY_DESCRIPTORS, handles, ttlList, false);
      initHandlerMap(handles);
    } catch (RocksDBException e) {
      throw new RMQException(e.getMessage(), e);
    }
  }
}
