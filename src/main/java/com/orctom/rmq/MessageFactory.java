package com.orctom.rmq;

import com.lmax.disruptor.EventFactory;

public class MessageFactory implements EventFactory<Message>{

  @Override
  public Message newInstance() {
    return new Message();
  }
}
