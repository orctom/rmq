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

Subscribing:
```
RMQ.getInstance().subscribe(topic, new RMQConsumer() {...});
```

Sending message to the Queue:
```
RMQ.getInstance().send(topic, "payload");
```

Changing default TTL:
```
RMQOptions.getInstance().setTtl(1000);
```
**It must be called before any calls to `RMQ.getInstance()`**

Closing it when shutting down:
```
RMQ.getInstance().close();
```
