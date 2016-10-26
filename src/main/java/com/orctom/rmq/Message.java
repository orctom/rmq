package com.orctom.rmq;

public class Message {

  private byte[] payload;

  public Message() {
  }

  public Message(byte[] payload) {
    this.payload = payload;
  }

  public byte[] getPayload() {
    return payload;
  }

  public void setPayload(byte[] payload) {
    this.payload = payload;
  }
}
