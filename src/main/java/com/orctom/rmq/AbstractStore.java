package com.orctom.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

abstract class AbstractStore {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  String getPath(String id) {
    return "data/" + id;
  }

  void ensureDataDirExist() {
    File dataDir = new File(".", "data");
    if (dataDir.exists()) {
      return;
    }
    boolean created = dataDir.mkdirs();
    logger.trace("ensuring data dir existence, created: {}", created);
  }
}
