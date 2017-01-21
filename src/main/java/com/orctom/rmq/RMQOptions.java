package com.orctom.rmq;

public class RMQOptions {

  static final String DEFAULT_ID = "default";
  static final int DEFAULT_TTL = 7200;

  /**
   * Optional ID that will be used as the parent directory of the data store.
   */
  private String id = DEFAULT_ID;

  /**
   * Default: 120 minutes<br/>
   * value is in <code>seconds</code>
   */
  private int ttl;

  private boolean batchMode;

  public RMQOptions(String id, int ttl, boolean batchMode) {
    if (null != id) {
      this.id = id.replaceAll("\\W", "_");
    }
    this.ttl = ttl;
    this.batchMode = batchMode;
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
}
