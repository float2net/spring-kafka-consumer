# spring-kafka-consumer
A kafka consumption framework based on spring-kafka with rate-limit, back-pressure, and multi-threaded distribution

## Functional Overview

1. This framework module handles rate-limit, back-pressure, and multi-threaded distribution in all.

2. Provides a base class of generic types, as well as consumer classes for the commonly used String and Byte[] types of Kafka records

3. Users can expand the consumption of other special types of kafka records based on the generic class

4. Users can choose to consume records in batches or one by one.

5. For detailed usage, please refer to the KafkaConsumerUsage class of the spring-kafka-consumer-test module

6. In addition, a service-api is provided to start and pause the kafka consumption task online. For specific usage, please refer to the KafkaConsumerController class of the spring-kafka-consumer-test module.

## Usage examples

1. ### Consumption of String type kafka records

1. Inject _StringKafkaConsumerService_

```java

@Resource

private StringKafkaConsumerService stringKafkaConsumerService;

```

2. Define configuration parameters (in actual applications, various configuration parameters are generally injected into the yml configuration through @Value)

```java

final Config myConfig = Config.builder()

.servers("127.0.0.1:9094") //kafka server address

.topics("someTopic") //Consumer topic

.groupId("someGroup") //Consumer group

.consumerConcurrency(2) //Number of concurrent threads receiving messages, defined as the same as the number of partitions of kafka topic

.processorConcurrency(10) //Number of concurrent threads processing messages, initially defined as consumerConcurrency, can be increased by multiples according to the backlog

.fetchMinBytes(1000) //kafka consumption parameters, the minimum number of bytes consumed each time, determines whether multiple records can be consumed at one time
.build();
```

3. Define a new _StringMessageProcessTask_ task and override the corresponding _process_ method according to the way you want to consume records.
```java
StringMessageProcessTask myTask = new StringMessageProcessTask("task-name", myConfig) {
/**
* If you want to process single records one by one, override this method
*/
@Override
protected void process(ConsumerRecord<String, String> record) {
//Message processing logic
}
/**
* If you want to process multiple records in batches, override this method
*/
@Override
protected void process(ConsumerRecords<String, String> records) {
//Message processing logic
}
};
```

4. Submit the task and take effect immediately.
```java
stringKafkaConsumerService.submitTask(myTask, true);
```

2. ### Consumption of Byte[] type kafka records
Similar to the consumption of String type, except that the classes used are **_BytesKafkaConsumerService_** and **_BytesMessageProcessTask_**

3. ### Consumption of other types of kafka records
Refer to the implementation of **_StringKafkaConsumerService_** and **_StringMessageProcessTask_**,
respectively extend the generic classes **_KafkaConsumerService<K, V>_** and **_AbstractMessageProcessTask<K, V>_** to implement.

---The End---