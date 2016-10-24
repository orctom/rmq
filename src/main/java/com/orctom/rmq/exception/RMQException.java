package com.orctom.rmq.exception;

public class RMQException extends FastException {

  public RMQException(String message) {
    super(message);
  }

  public RMQException(String message, Throwable cause) {
    super(message, cause);
  }
}
