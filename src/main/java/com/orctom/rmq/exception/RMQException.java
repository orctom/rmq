package com.orctom.rmq.exception;

import org.rocksdb.RocksDBException;

public class RMQException extends FastException {

  public RMQException(RocksDBException e) {
    super(e.getMessage() + " (" + e.getStatus().getCodeString() + ")", e);
  }
}
