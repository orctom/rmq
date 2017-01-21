## RMQ

A simple and embedded message queue, that:

 * Only can be operated by one process.
 * Supports persistence (into hard drive).
 * Supports TTL (messages will be deleted in 2 hours by default, configurable).

### How to use it

Add it to your project, e.g. in Maven:
```
    <dependency>
      <groupId>com.orctom</groupId>
      <artifactId>rmq</artifactId>
      <version>${rmq.version}</version>
    </dependency>
```

Acquiring a RMQ instance (multiton):
```
RMQ rmq = RMQ.getInstance();

// or
RMQOptions options = new RMQOptions(
    "dummy", // id
    1000,    // ttl in seconds
    false    // batch mode persistent
);
RMQ rmq = RMQ.getInstance(options);
```

Subscribing:
```
rmq.subscribe(topic, new RMQConsumer() {...});
```

Sending message to the Queue:
```
RMQ.getInstance().send(topic, "payload");
```

Closing it when shutting down:
```
RMQ.getInstance().close();
```
