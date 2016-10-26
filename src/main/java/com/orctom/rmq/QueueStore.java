package com.orctom.rmq;

import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

class QueueStore extends AbstractStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueStore.class);

  public static final int PERSIST_DELAY = 0;
  public static final int PERSIST_PERIOD = 1000;

  private final TtlDB db;
  private final DBOptions options = new DBOptions();
  private final WriteOptions writeOptions = new WriteOptions();

  private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

  QueueStore(List<String> queueNames, int ttl) {
    ensureDataDirExist();
    columnFamilyDescriptors = createColumnFamilyDescriptors(queueNames);

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

  private void initBatchThread() {
    Timer timer = new Timer(false);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        LOGGER.info("Writing batch...");
        writeLock.lock();
        try {
          for (ColumnFamilyDescriptor descriptor : columnFamilyDescriptors) {
            WriteBatch writeBatch = writeBatches.put(descriptor, new WriteBatch());
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

  public void close() {
    columnFamilyHandles.values().forEach(ColumnFamilyHandle::close);
    options.close();
    if (null != db) {
      db.close();
    }
  }

  void close() {
    options.close();
    if (null != db) {
      db.close();
    }
  }
}
