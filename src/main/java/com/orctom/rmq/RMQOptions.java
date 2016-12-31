package com.orctom.rmq;

public class RMQOptions {

  /**
   * Default: 90 minutes<br/>
   * value is in <code>seconds</code>
   */
  private int ttl = 5400;

  /**
   * Default: 5000 messages per second * 2 hours
   */
  private int capacity = 36_000_000;

  public RMQOptions setTtl(int ttl) {
    this.ttl = ttl;
    return this;
  }

  public RMQOptions setCapacity(int capacity) {
    this.capacity = capacity;
    return this;
  }

  public int getTtl() {
    return ttl;
  }

  public int getCapacity() {
    return capacity;
  }
}
