package float2net.framework.springkafkaconsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * kafka consumer service (generic type)
 */
@Slf4j
public class KafkaConsumerServiceBase<K, V> {

    private final Map<String, AbstractMessageProcessTask<K, V>> tasksHolder = new ConcurrentHashMap<>();

    private final String keyDeserializerClassName;
    private final String valueDeserializerClassName;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    //TODO @AllArgsConstructor
    public KafkaConsumerServiceBase(String keyDeserializerClassName, String valueDeserializerClassName,
                                    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.keyDeserializerClassName = keyDeserializerClassName;
        this.valueDeserializerClassName = valueDeserializerClassName;
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        if (log.isInfoEnabled()) {
            log.info("construct with '{}' and '{}'", keyDeserializerClassName, valueDeserializerClassName);
        }
    }

    public Collection<AbstractMessageProcessTask<K, V>> findAllTasks() {
        return tasksHolder.values().stream().peek(task ->
                Optional.ofNullable(kafkaListenerEndpointRegistry.getListenerContainer(task.getTaskId()))
                        .ifPresent(container -> {
                            task.setRunning(container.isRunning());
                            task.setPaused(container.isContainerPaused());
                        })).collect(Collectors.toSet());
    }

    /* FIXME: pause/resume 未见效果 */
    public boolean pauseTask(String taskId) {
        Assert.hasText(taskId, "taskId不能为空");
        final MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(taskId);
        if (container != null) {
            container.pause();
            return true;
        } else {
            return false;
        }
    }

    /* FIXME: pause/resume 未见效果 */
    public boolean resumeTask(String taskId) {
        Assert.hasText(taskId, "taskId不能为空");
        final MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(taskId);
        if (container != null) {
            container.resume();
            return true;
        } else {
            return false;
        }
    }

    public boolean startTask(String taskId) {
        Assert.hasText(taskId, "taskId不能为空");
        final MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(taskId);
        if (container != null) {
            container.start();
            return true;
        } else {
            return false;
        }
    }

    public boolean stopTask(String taskId) {
        Assert.hasText(taskId, "taskId不能为空");
        final MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(taskId);
        if (container != null) {
            container.stop(); //stop后registry中还存在这个container,只是没在运行了，但是还可以再start。
            return true;
        } else {
            return false;
        }
    }

    //stop后registry中还存在这个container，还可以再start, 因此这里remove没意义。
//    public boolean removeTask(String taskId) {
//        final MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(taskId);
//        if (container != null) {
//            container.stop(() -> tasksHolder.remove(taskId));
//            return true;
//        } else {
//            return false;
//        }
//    }


    /**
     * create a KafkaConsumerFactory
     *
     * @param processTask
     * @return
     */
    private ConsumerFactory<K, V> consumerFactory(AbstractMessageProcessTask<K, V> processTask) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, processTask.config.getServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); //参考：https://docs.spring.io/spring-kafka/reference/html/#committing-offsets
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializerClassName);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializerClassName);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, processTask.config.getGroupId());
        Optional.ofNullable(processTask.config.getAutoOffsetReset()).ifPresent(x -> props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, x));
        Optional.ofNullable(processTask.config.getEnableAutoCommit()).ifPresent(x -> props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, x));
        Optional.ofNullable(processTask.config.getAutoCommitIntervalMs()).ifPresent(x -> props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, x));
        Optional.ofNullable(processTask.config.getFetchMaxBytes()).ifPresent(x -> props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, x));
        Optional.ofNullable(processTask.config.getFetchMinBytes()).ifPresent(x -> props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, x));
        Optional.ofNullable(processTask.config.getFetchMaxWaitMs()).ifPresent(x -> props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, x));
        Optional.ofNullable(processTask.config.getMaxPartitionFetchBytes()).ifPresent(x -> props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, x));
        Optional.ofNullable(processTask.config.getSessionTimeoutMs()).ifPresent(x -> props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, x));
        Optional.ofNullable(processTask.config.getMaxPollIntervalMs()).ifPresent(x -> props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, x));
        Optional.ofNullable(processTask.config.getMaxPollRecords()).ifPresent(x -> props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, x));
        Optional.ofNullable(processTask.config.getProps()).ifPresent(props::putAll);
        /**
         * 处理异常的kafka消息, 参考:
         * https://blog.csdn.net/aw9165628/article/details/111308413
         * https://stackoverflow.com/questions/53327998/how-to-skip-corrupt-non-serializable-messages-in-spring-kafka-consumer
         *
         * FIXME: 启动时会有warn警告，据说是和kafka版本低有关系:
         * https://stackoverflow.com/questions/55455709/kafka-connect-the-configuration-xxx-was-supplied-but-isnt-a-known-config-in-ad
         */
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, ByteArrayDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, ByteArrayDeserializer.class.getName());
        props.put(ErrorHandlingDeserializer.KEY_FUNCTION, FailedValueDeserializationHandler.class);
        props.put(ErrorHandlingDeserializer.VALUE_FUNCTION, FailedValueDeserializationHandler.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * kafka record value deserialization failing handler
     */
    public static class FailedValueDeserializationHandler implements Function<FailedDeserializationInfo, String> {
        public String apply(FailedDeserializationInfo info) {
            final String corruptData = Arrays.toString(info.getData());
            String errMsg = "接收到无法反序列化的kafka消息将被忽略: " + corruptData;
            log.error(errMsg);
            return errMsg;
        }
    }

    /**
     * 提交kafka消费任务, 暂时不启动
     *
     * @param processTask
     */
    public void submitTask(AbstractMessageProcessTask<K, V> processTask) {
        submitTask(processTask, false);
    }

    /**
     * 提交kafka消费任务，并立即启动
     *
     * @param processTask
     */
    @SneakyThrows(value = {NoSuchMethodException.class})
    public void submitTask(AbstractMessageProcessTask<K, V> processTask, boolean startImmediately) {
        final String taskId = processTask.getTaskId();
        Assert.hasText(taskId, "taskId不能为空");
        Optional.ofNullable(kafkaListenerEndpointRegistry.getListenerContainer(taskId)).ifPresent(v -> {
            throw new IllegalArgumentException("该taskId已经注册过了，不可重复使用: " + taskId);
        });

        /* 检查topics合法性 */
        final String[] topics = Optional.ofNullable(processTask.config.getTopics())
                .map(x -> x.split(",")).orElse(null);
        Assert.notEmpty(topics, "kafka消费主题不能为空");

        /* create a kafka listener container factory */
        ConcurrentKafkaListenerContainerFactory<K, V> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory(processTask));
        Integer consumerConcurrency = Optional.ofNullable(processTask.config.getConsumerConcurrency()).filter(n -> n > 0).orElse(1);
        listenerContainerFactory.setConcurrency(consumerConcurrency);
        //>>>可自定义一些容器属性
        //spring-kafka的offset提交策略
//        listenerContainerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        //rebalance之前提交一次offset,避免重复消费
        listenerContainerFactory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                ConsumerAwareRebalanceListener.super.onPartitionsRevokedBeforeCommit(consumer, partitions);
                log.warn("partition is to be revoked, before that, we commit offset...");
                consumer.commitSync();
            }
        });
        //<<<
        /* create a method kafka listener endpoint */
        MethodKafkaListenerEndpoint<K, V> methodEndpoint = new MethodKafkaListenerEndpoint<>();
        methodEndpoint.setId(taskId);
        methodEndpoint.setAutoStartup(startImmediately);
        methodEndpoint.setTopics(topics);
        methodEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        methodEndpoint.setBean(new KafkaMessageListener<>(processTask)); /* 创建一个新的消息监听器 */
        methodEndpoint.setMethod(KafkaMessageListener.class.getMethod("onMessage", ConsumerRecords.class)); /* 指定接收kafka消息的方法 */
        methodEndpoint.setBatchListener(true); /* 指定批量接收kafka消息 */
        methodEndpoint.setErrorHandler((message, exception) -> {
            log.error("接收kafka消息异常: {}", message, exception);
            return null;
        });
        /* register the endpoint with the factory, and start to receive message immediately */
        kafkaListenerEndpointRegistry.registerListenerContainer(methodEndpoint, listenerContainerFactory, startImmediately);

        /* hold the task in local map */
        tasksHolder.put(taskId, processTask);
    }

}
