# spring-kafka-consumer
一个基于spring-kafka的具备限流、反压、多线程分发的kafka消费框架

## 功能概述

1. 模块内部统一处理了限流、反压、多线程分发。
2. 提供泛型类型的基类，以及常用的String和Byte[]两种类型Kafka记录的消费类
3. 用户可以自行在泛型类基础上扩展其他特殊类型kafka记录的消费
4. 用户可以选择批量消费记录或者逐个消费记录。
5. 详细用法可以参考spring-kafka-consumer-test模块的KafkaConsumerUsage类
6. 另外提供了在线对kafka消费任务启动和暂停的service-api，具体用法可以参考spring-kafka-consumer-test模块的KafkaConsumerController类。

## 用法举例

1. ### String类型的kafka记录的消费
    1. 注入 _StringKafkaConsumerService_
   ```java
   @Resource
   private StringKafkaConsumerService stringKafkaConsumerService;
   ```

    2. 定义配置参数（实际应用中，各项配置参数一般通过@Value来注入yml的配置）
   ```java
   final Config myConfig = Config.builder()
           .servers("127.0.0.1:9094") //kafka服务器地址
           .topics("someTopic") //消费主题
           .groupId("someGroup") //消费组
           .consumerConcurrency(2) //接收消息的并发线程数,定义为kafka topic的partition数相同
           .processorConcurrency(10) //处理消息的并发线程数，初始可以定义为和consumerConcurrency，根据积压情况，可以按倍数加大
           .fetchMinBytes(1000) //kafka消费参数, 每次消费的最少字节数，决定能否一次性消费多条记录
           .build(); 
   ```

    3. 定义一个新的 _StringMessageProcessTask_ 任务，并根据希望消费记录的方式，重写相应的 _process_ 方法。
   ```java
   StringMessageProcessTask myTask = new StringMessageProcessTask("task-name", myConfig) {
           /**
            * 如果希望逐个处理单条记录，则重写此方法
            */
           @Override
           protected void process(ConsumerRecord<String, String> record) {
                //消息处理逻辑
            }
           /**
            * 如果希望批量处理多条记录，则重写此方法
            */
            @Override
            protected void process(ConsumerRecords<String, String> records) {
                //消息处理逻辑
            }
        };
   ```

    4. 提交任务并立即生效。
   ```java
   stringKafkaConsumerService.submitTask(myTask, true);
   ```

2. ### Byte[]类型的kafka记录的消费
与String类型的消费类似，只不过使用的类为 **_BytesKafkaConsumerService_** 和 **_BytesMessageProcessTask_**

3. ### 其他类型的kafka记录的消费
参照 **_StringKafkaConsumerService_** 和 **_StringMessageProcessTask_** 的实现方式，
分别扩展泛型类 **_KafkaConsumerService<K, V>_** 和 **_AbstractMessageProcessTask<K, V>_** 去实现。

---完---