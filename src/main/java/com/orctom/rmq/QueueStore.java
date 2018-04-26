package com.orctom.rmq;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.orctom.laputa.utils.IdGenerator;
import com.orctom.rmq.exception.RMQException;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

class QueueStore extends AbstractStore implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueueStore.class);

  private static final String NAME = "queues";
  private static final String ROCKSDB_ESTIMATE_NUM_KEYS = "rocksdb.estimate-num-keys";

  private final MetaStore metaStore;
  private final int ttl;
  private final TtlDB db;
  private final Options options = new Options().setCreateIfMissing(true);
  private final DBOptions dbOptions = new DBOptions();
  private final WriteOptions writeOptions = new WriteOptions();

  private final IdGenerator idGenerator = IdGenerator.create();

  private ExecutorService es;
  private Map<String, Queue> queues = new ConcurrentHashMap<>();

  // for batch mode
  private Map<String, WriteBatch> batches = new ConcurrentHashMap<>();
  private Timer timer;
  private boolean isBatch;

  // ============================= constructors ============================

  QueueStore(MetaStore metaStore, RMQOptions rmqOptions) {
    this.ttl = rmqOptions.getTtl();
    this.metaStore = metaStore;

    initExecutorService(rmqOptions.getId());

    try {
      db = TtlDB.open(options, getPath(rmqOptions.getId(), NAME), rmqOptions.getTtl(), false);
      setupBatchThread(rmqOptions);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  QueueStore(MetaStore metaStore, List<String> queueNames, RMQOptions rmqOptions) {
    this.ttl = rmqOptions.getTtl();
    this.metaStore = metaStore;
    if (null == queueNames) {
      throw new IllegalArgumentException("QueueNames should not be null");
    }

    initExecutorService(rmqOptions.getId());

    List<ColumnFamilyDescriptor> descriptors = createColumnFamilyDescriptors(queueNames);
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<Integer> ttlList = createTTLs(queueNames.size(), rmqOptions.getTtl());
    try {
      db = TtlDB.open(dbOptions, getPath(rmqOptions.getId(), NAME), descriptors, handles, ttlList, false);
      initQueues(queueNames, descriptors, handles);
      setupBatchThread(rmqOptions);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  private void initExecutorService(String id) {
    es = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat("rmq-queue-" + id + "-%d").build()
    );
  }

  // ============================= batches ============================

  private void setupBatchThread(RMQOptions rmqOptions) {
    isBatch = rmqOptions.isBatchMode();
    if (!isBatch) {
      return;
    }

    int period = rmqOptions.getBatchPeriod();
    timer = new Timer(false);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        LOGGER.info("Writing batch...");
        for (String queueName : queues.keySet()) {
          WriteBatch writeBatch = batches.put(queueName, new WriteBatch());
          persist(writeBatch);
        }
      }
    }, period, period);
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
      throw new RMQException(e);
    }
  }

  private WriteBatch getWriteBatch(String queueName) {
    return batches.computeIfAbsent(queueName, name -> new WriteBatch());
  }

  // ============================= queue apis ============================

  private Queue createQueue(String queueName) {
    try {
      ColumnFamilyDescriptor descriptor = createColumnFamilyDescriptor(queueName);
      ColumnFamilyHandle handle = db.createColumnFamilyWithTtl(descriptor, ttl);
      Queue queue = new Queue(queueName, descriptor, handle, metaStore, this);
      metaStore.queueCreated(queueName);
      return queue;
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  void deleteQueue(String queueName) {
    Queue queue = queues.remove(queueName);
    dropColumnFamily(queue.getHandle());
    try {
      queue.close();
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    metaStore.queueDeleted(queueName);
  }

  private Queue getQueue(String name) {
    return queues.computeIfAbsent(name, this::createQueue);
  }

  void subscribe(String queueName, RMQConsumer... consumers) {
    if (null == consumers) {
      return;
    }

    Queue queue = getQueue(queueName);
    startQueue(queue);
    queue.addConsumers(consumers);
  }

  void unsubscribe(String queueName, RMQConsumer... consumers) {
    if (null == consumers) {
      return;
    }

    Queue queue = queues.get(queueName);
    if (null == queue) {
      return;
    }
    queue.removeConsumers(consumers);
  }

  private void startQueue(Queue queue) {
    if (queue.isStarted()) {
      return;
    }
    queue.setStarted();
    es.submit(queue);
    LOGGER.info("[{}} started.", queue.getName());
    try {
      TimeUnit.MILLISECONDS.sleep(50);
    } catch (InterruptedException ignored) {
    }
  }

  void push(String queueName, String message) {
    String id = String.valueOf(idGenerator.generate());
    push(queueName, id, message);
  }

  void push(String queueName, String id, String message) {
    Queue queue = getQueue(queueName);
    LOGGER.trace("[{}] pushed, {}: {}", queueName, id, message);
    push(queue, id, message);
  }

  void push(String queueName, Message message) {
    Queue queue = getQueue(queueName);
    LOGGER.trace("[{}] pushed, {}", queueName, message);
    push(queue, message);
  }

  void delete(String queueName, String id) {
    Queue queue = queues.get(queueName);
    if (null == queue) {
      return;
    }
    delete(queue, id);
  }

  void flush(String queueName, String upperBound) {
    Queue queue = queues.get(queueName);
    if (null == queue) {
      return;
    }
    flush(queue, upperBound);
  }

  Long getSize(String queueName) {
    Queue queue = queues.get(queueName);
    if (null == queue) {
      return 0L;
    }

    try {
      long size = db.getLongProperty(queue.getHandle(), ROCKSDB_ESTIMATE_NUM_KEYS);
      return size <= 1 ? 0: size;
    } catch (RocksDBException e) {
      return null;
    }
  }

  RocksIterator iter(String queueName) {
    Queue queue = queues.get(queueName);
    if (null == queue) {
      return null;
    }

    return positionedIterator(queue);
  }

  RocksIterator positionedIterator(Queue queue) {
    RocksIterator iterator = iter(queue);
    if (null == iterator) {
      return null;
    }
    String offset = metaStore.getOffset(queue.getName());
    if (Strings.isNullOrEmpty(offset)) {
      iterator.seekToFirst();
    } else {
      byte[] offsetBytes = offset.getBytes();
      iterator.seek(offsetBytes);
      if (Arrays.equals(offsetBytes, iterator.key())) {
        iterator.next();
      }
    }
    return iterator;
  }

  // ============================= low level apis ============================

  private RocksIterator iter(Queue queue) {
    return db.newIterator(queue.getHandle());
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
    List<Integer> ttlList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      ttlList.add(ttl);
    }
    return ttlList;
  }

  private void initQueues(List<String> queueNames,
                          List<ColumnFamilyDescriptor> descriptors,
                          List<ColumnFamilyHandle> handles) {
    for (int i = 0; i < handles.size(); i++) {
      String queueName = queueNames.get(i);
      ColumnFamilyDescriptor descriptor = descriptors.get(i);
      ColumnFamilyHandle handle = handles.get(i);
      Queue queue = new Queue(queueName, descriptor, handle, metaStore, this);
      queues.put(queueName, queue);
    }
  }

  private void push(Queue queue, String key, String value) {
    push(queue, key.getBytes(), value.getBytes());
  }

  private void push(Queue queue, Message message) {
    push(queue, message.getId().getBytes(), message.getData());
  }

  private void push(Queue queue, byte[] key, byte[] value) {
    try {
      if (isBatch) {
        getWriteBatch(queue.getName()).put(queue.getHandle(), key, value);
      } else {
        db.put(queue.getHandle(), writeOptions, key, value);
      }
      queue.signalNewMessage();
    } catch (RocksDBException e) {
      LOGGER.error("queue: {}, key: {}, value: {}", queue.getName(), new String(key), new String(value));
      throw new RMQException(e);
    }
  }

  private void delete(Queue queue, String key) {
    delete(queue, key.getBytes());
  }

  private void delete(Queue queue, byte[] key) {
    try {
      if (isBatch) {
        getWriteBatch(queue.getName()).remove(queue.getHandle(), key);
      } else {
        db.delete(queue.getHandle(), writeOptions, key);
      }
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  String get(Queue queue, String key) {
    return new String(get(queue, key.getBytes()));
  }

  byte[] get(Queue queue, byte[] key) {
    try {
      return db.get(queue.getHandle(), key);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  void move(String key, Queue sourceQueue, Queue targetQueue) {
    move(key.getBytes(), sourceQueue, targetQueue);
  }

  void move(byte[] key, Queue sourceQueue, Queue targetQueue) {
    byte[] value = get(sourceQueue, key);
    delete(sourceQueue, key);
    push(targetQueue, key, value);
  }

  private void flush(Queue queue, String upperBound) {
    try {
      WriteBatch batch = new WriteBatch();
      RocksIterator iterator = iter(queue);
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        byte[] key = iterator.key();
        if (isGreaterEqualThan(new String(key), upperBound)) {
          break;
        }
        batch.remove(queue.getHandle(), key);
      }
      db.write(writeOptions, batch);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  private void dropColumnFamily(ColumnFamilyHandle handle) {
    if (null == handle) {
      return;
    }

    try {
      db.dropColumnFamily(handle);
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  void updateMeta() {
    LOGGER.trace("Cleaning deleted messages");
    for (Queue queue : queues.values()) {
      try {
        cleanOffsets(queue);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
  }

  private void cleanOffsets(Queue queue) {
    String queueName = queue.getName();
    String offset = metaStore.getOffset(queueName);
    if (Strings.isNullOrEmpty(offset)) {
      return;
    }
    Long start = System.currentTimeMillis();
    clean(queue, queueName, offset);
    long end = System.currentTimeMillis();
    LOGGER.debug("[{}] cleaned took: {} ms", queueName, (end - start));
  }

  private void clean(Queue queue, String queueName, String offset) {
    LOGGER.trace("[{}] cleaning messages before offset: {} ", queueName, offset);
    RocksIterator iterator = db.newIterator(queue.getHandle());
    long size = 0;
    WriteBatch batch = new WriteBatch();
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      String key = new String(iterator.key());
      if (isGreaterEqualThan(key, offset)) {
        continue;
      }

      try {
        batch.remove(queue.getHandle(), iterator.key());
      } catch (RocksDBException e) {
        throw new RMQException(e);
      }
    }

    try {
      if (batch.count() > 0) {
        db.write(writeOptions, batch);
      }
    } catch (RocksDBException e) {
      throw new RMQException(e);
    }
  }

  private boolean isGreaterEqualThan(String key, String offset) {
    int len1 = key.length();
    int len2 = offset.length();
    if (len1 == len2) {
      return key.compareTo(offset) >= 0;
    } else {
      return len1 > len2;
    }
  }

  @Override
  public void close() {
    LOGGER.debug("Closing QueueStore...");
    if (null != timer) {
      timer.cancel();
    }
    shutdownExecutorService();
    options.close();
    dbOptions.close();
    if (null != db) {
      db.close();
    }
    metaStore.close();
    LOGGER.debug("Closed QueueStore.");
  }

  private void shutdownExecutorService() {
    es.shutdown();
    try {
      es.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
    }
    es.shutdownNow();
    queues.clear();
  }
}
