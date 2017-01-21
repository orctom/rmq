package com.orctom.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

abstract class AbstractStore {

  private static final String ROOT_DIR = ".data";
  private final Logger logger = LoggerFactory.getLogger(getClass());

  String getPath(String id, String name) {
    String path = ROOT_DIR + File.separator + id + File.separator + name;
    logger.trace("Path for storage: {}", path);
    ensureDataDirExist(path);
    return path;
  }

  protected void ensureDataDirExist(String path) {
    File dataDir = new File(path).getParentFile();
    if (dataDir.exists()) {
      return;
    }
    boolean created = dataDir.mkdirs();
    logger.trace("Ensuring data dir existence, created: {}", created);
  }

  private String getDataDirPath(String id) {
    return ROOT_DIR + File.separator + id + File.separator;
  }
}
