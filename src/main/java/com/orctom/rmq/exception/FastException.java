package com.orctom.rmq.exception;

public class FastException extends RuntimeException {

  public FastException() {
  }

  public FastException(Throwable cause) {
    super(cause);
  }

  public FastException(String message) {
    super(message);
  }

  public FastException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public Throwable fillInStackTrace() {
    return null;
  }
}
