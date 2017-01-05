package com.orctom.rmq;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

abstract class AbstractStore {

  private static final String ROOT_DIR = ".data";
  private final Logger logger = LoggerFactory.getLogger(getClass());

  String getPath(String name) {
    String path = getDataDirPath() + name;
    logger.trace("Path for storage: {}", path);
    return path;
  }

  protected void ensureDataDirExist() {
    File dataDir = new File(getDataDirPath());
    if (dataDir.exists()) {
      return;
    }
    boolean created = dataDir.mkdirs();
    logger.trace("Ensuring data dir existence, created: {}", created);
  }

  private String getDataDirPath() {
    String id = RMQOptions.getInstance().getId();
    if (Strings.isNullOrEmpty(id)) {
      return ROOT_DIR + File.separator;
    } else {
      return ROOT_DIR + File.separator + id + File.separator;
    }
  }
}
