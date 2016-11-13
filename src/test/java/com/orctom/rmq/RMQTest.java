package com.orctom.rmq;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RMQTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQTest.class);

  @Test
  public void test() {
    DummyConsumer consumer = new DummyConsumer();
    String topic = "events";
    RMQ rmq = RMQ.getInstance();
    rmq.subscribe(topic, consumer);
    for (int i = 0; i < 1_000_000; i++) {
      rmq.send(topic, RandomStringUtils.randomAlphanumeric(100));
    }
  }
}
