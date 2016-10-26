package com.orctom.rmq;

import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class ContensionTest {

  @Test
  public void testLoop() {
    AtomicLong count = new AtomicLong(0);
    Stopwatch sw = Stopwatch.createStarted();
    for (int i = 0; i < 500_000_000; i++) {
      count.incrementAndGet();
    }
    sw.stop();
    System.out.println(sw.toString());
  }
}
