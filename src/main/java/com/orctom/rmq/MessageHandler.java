package com.orctom.rmq;

import com.lmax.disruptor.EventHandler;

public class MessageHandler implements EventHandler<Message> {

  @Override
  public void onEvent(Message message, long seq, boolean isEnd) throws Exception {

  }
}
