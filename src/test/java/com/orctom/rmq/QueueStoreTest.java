package com.orctom.rmq;

import com.google.common.collect.Lists;
import org.junit.Test;

public class QueueStoreTest {

  private QueueStore store = new QueueStore(Lists.newArrayList("dummy"), 30);

  @Test
  public void test() {
  }
}
