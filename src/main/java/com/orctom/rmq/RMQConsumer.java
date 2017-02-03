package com.orctom.rmq;

public interface RMQConsumer {

  Ack onMessage(Message message);

}
