package com.orctom.rmq;

import java.io.Serializable;
import java.util.Base64;

public class Message implements Serializable {

  protected String id;
  protected byte[] data;

  public Message(String id, byte[] data) {
    this.id = id;
    this.data = data;
  }

  public Message(Message message) {
    this.id = message.getId();
    this.data = message.getData();
  }

  public String getId() {
    return id;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return id + ", " + Base64.getEncoder().encodeToString(data);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Message message = (Message) o;

    return id.equals(message.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
