package com.orctom.rmq;

public class RMQOptions {

  private static final RMQOptions INSTANCE = new RMQOptions();

  /**
   * Default: 90 minutes<br/>
   * value is in <code>seconds</code>
   */
  private int ttl = 5400;

  /**
   * Optional ID that will be used as the parent directory of the data store.
   */
  private String id;

  private RMQOptions() {}

  public static RMQOptions getInstance() {
    return INSTANCE;
  }

  public int getTtl() {
    return ttl;
  }

  public RMQOptions setTtl(int ttl) {
    this.ttl = ttl;
    return this;
  }

  public String getId() {
    return id;
  }

  public RMQOptions setId(String id) {
    if (null != id) {
      this.id = id.replaceAll("\\W", "_");
    }
    return this;
  }
}
