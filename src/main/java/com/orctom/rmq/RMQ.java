package com.orctom.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RMQ {

  private static final Logger LOGGER = LoggerFactory.getLogger(RMQ.class);

  private static final RMQ INSTANCE = new RMQ();

  private MetaStore metaStore;
  private QueueStore queuesStore;

  private RMQ() {
    metaStore = MetaStore.getInstance();
    readMetaInfo();
  }

  private void readMetaInfo() {
    Map<String, String> queues =  metaStore.getAll();
    
  }

}
