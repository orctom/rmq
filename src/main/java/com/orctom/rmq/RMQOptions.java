package com.orctom.rmq;

public class RMQOptions {

  private static final String DEFAULT_ID = "default";
  private static final int DEFAULT_TTL = 7200;
  private static final int DEFAULT_BATCH_PERIOD = 1000;

  /**
   * Optional ID that will be used as the parent directory of the data store.
   */
  private String id = DEFAULT_ID;

  /**
   * Messages will be erased after this ttl (in seconds).
   * Default: 2 hours.
   */
  private int ttl;

  /**
   * Whether messages be persisted to disk in batch mode with a fixed period.
   */
  private boolean batchMode;

  private int batchPeriod;

  public RMQOptions() {
    this(DEFAULT_ID);
  }

  public RMQOptions(String id) {
    this(id, DEFAULT_TTL);
  }

  public RMQOptions(String id, int ttl) {
    this(id, ttl, false);
  }

  public RMQOptions(String id, int ttl, boolean batchMode) {
    this(id, ttl, batchMode, DEFAULT_BATCH_PERIOD);
  }

  public RMQOptions(String id, int ttl, boolean batchMode, int batchPeriod) {
    if (null != id) {
      this.id = id.replaceAll("\\W", "_");
    }
    this.ttl = ttl;
    this.batchMode = batchMode;
    this.batchPeriod = batchPeriod;
  }

  String getId() {
    return id;
  }

  int getTtl() {
    return ttl;
  }

  public boolean isBatchMode() {
    return batchMode;
  }

  public int getBatchPeriod() {
    return batchPeriod;
  }
}
