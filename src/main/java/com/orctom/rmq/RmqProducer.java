package com.orctom.rmq;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RmqProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RmqProducer.class);

  private static final RmqProducer INSTANCE = new RmqProducer();

  private final RingBuffer<Message> ringBuffer;

  private RMQ rmq;

  private RmqProducer() {
    rmq = new RMQ("rmq");
    ringBuffer = new RingBuffer<>();
  }

  public static RmqProducer getInstance() {
    return INSTANCE;
  }

  public void send(String topic, String data) {
    send(topic, data.getBytes());
  }

  public void send(String topic, byte[] data) {
    long seq = ringBuffer.next();
    Message msg = ringBuffer.get(seq);
    msg.setPayload(data);
    ringBuffer.publish(seq);
  }
}
