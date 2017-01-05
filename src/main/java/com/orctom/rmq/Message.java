package com.orctom.rmq;

import java.io.Serializable;

public class Message implements Serializable {

  private String id;
  private String value;

  public Message(String id, String value) {
    this.id = id;
    this.value = value;
  }

  public String getId() {
    return id;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return id + ": " + value;
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
