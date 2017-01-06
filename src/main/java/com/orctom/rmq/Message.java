package com.orctom.rmq;

import java.io.Serializable;

public class Message implements Serializable {

  private String id;
  private byte[] data;

  public Message(String id, byte[] data) {
    this.id = id;
    this.data = data;
  }

  public String getId() {
    return id;
  }

  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return id + ": " + new String(data);
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
