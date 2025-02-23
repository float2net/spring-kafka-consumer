package float2net.framework.springkafkaconsumer;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 消息处理任务抽象类，使用者需要实现其中的<b>process</b>方法用于处理接收到的消息
 */
@Slf4j
@Getter
@Setter
public abstract class AbstractMessageProcessTask<K, V> implements Consumer<ConsumerRecords<K, V>> {

    private static final AtomicLong procCount = new AtomicLong(0);  //统计的量
    private static final AtomicLong procSec = new AtomicLong(System.currentTimeMillis() / 1000);  //统计的秒

    /**
     * 任务ID，标记该任务的唯一标识，主要用于日志输出
     */
    private final String taskId;

    /**
     * 任务配置参数
     */
    public final Config config;

    /**
     * 任务当前是否在运行中(这里的运行中的状态也可能是处于暂停中)
     */
    private boolean isRunning;

    /**
     * 任务当前是否已暂停
     */
    private boolean isPaused;

    public AbstractMessageProcessTask(String taskId, Config config) {
        Assert.hasText(taskId, "taskId不能为空");
        Assert.notNull(config, "Config不能为空");
        this.taskId = taskId;
        this.config = config;
    }

    /**
     * 接受消息并处理
     *
     * @param records kafka消息
     */
    @Override
    public void accept(ConsumerRecords<K, V> records) {
        Assert.isTrue(records != null && !records.isEmpty(), "kafka记录不能为空");
        long startTime = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug("开始处理{}条消息({})", records.count(), String.format("%08X", records.hashCode()));
        }
        process(records);
        if (log.isDebugEnabled()) {
            log.debug("处理完毕{}条消息({}), 总共耗时{}毫秒", records.count(),
                    String.format("%08X", records.hashCode()), (System.currentTimeMillis() - startTime));
        }
        //>>>统计秒计数
        if (log.isTraceEnabled()) {
            final long sec = System.currentTimeMillis() / 1000;
            if (sec != procSec.get()) {
                log.trace("前{}秒共处理完毕{}条消息", sec - procSec.get(), procCount.get());
                procCount.set(records.count());
                procSec.set(sec);
            } else {
                procCount.addAndGet(records.count());
            }
        }
        //<<<
    }

    /**
     * 需要实现这个消息处理方法
     *
     * @param records 待处理的kafka消息
     */
    protected abstract void process(ConsumerRecords<K, V> records);

    /**
     * 任务配置参数
     */
    @Getter
    @Builder
    public static class Config {

        /**
         * kafka服务器地址，多个地址之间逗号分隔
         */
        private final String servers;

        /**
         * 消费主题，多个主题之间逗号分隔
         */
        private final String topics;

        /**
         * 消费组ID
         */
        private final String groupId;

        /**
         * 每个消费并发线程每秒钟的最大消费记录数(限流),缺省值为0表示不限流
         */
        private final Integer rateLimit;

        /**
         * 消费消息线程的并发数，一般设置为kafka分区数量，缺省值为1
         */
        private final Integer consumerConcurrency;

        /**
         * 处理消息线程的并发数，根据消息处理耗时来确定，缺省值为10
         */
        private final Integer processorConcurrency;

        /* 下面是一些常用的 kafka consumer 配置参数 */

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_auto.offset.reset
         * <p>
         * What to do when there is no initial offset in Kafka or if the current offset does not exist any more
         * on the server (e.g. because that data has been deleted):
         * <p>
         * earliest: automatically reset the offset to the earliest offset
         * latest: automatically reset the offset to the latest offset
         * none: throw exception to the consumer if no previous offset is found for the consumer's group
         * anything else: throw exception to the consumer.
         * <p>
         * Type:	        string
         * Default:	        latest
         * Valid Values:    [latest, earliest, none]
         * Importance:	    medium
         */
        private final String autoOffsetReset;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit
         * <p>
         * If true the consumer's offset will be periodically committed in the background.
         * <p>
         * Type:	        boolean
         * Default:	        true
         * Valid Values:
         * Importance:	    medium
         */
        private final Boolean enableAutoCommit;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_auto.commit.interval.ms
         * <p>
         * The frequency in milliseconds that the consumer offsets are auto-committed to Kafka
         * if enable.auto.commit is set to true.
         * <p>
         * Type:	        int
         * Default:	        5000 (5 seconds)
         * Valid Values:	[0,...]
         * Importance:	    low
         */
        private final Integer autoCommitIntervalMs;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_fetch.max.bytes
         * <p>
         * The maximum number of bytes we will return for a fetch request. Must be at least 1024.
         * <p>
         * Type:	        int
         * Default:	        57671680 (55 megabytes)
         * Valid Values:    [1024,...]
         * Importance:	    medium
         * Update Mode:	    read-only
         */
        private final Integer fetchMaxBytes;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_fetch.min.bytes
         * <p>
         * The minimum amount of data the server should return for a fetch request. If insufficient data is available
         * the request will wait for that much data to accumulate before answering the request. The default setting
         * of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch
         * request times out waiting for data to arrive. Setting this to something greater than 1 will cause the server
         * to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost of
         * some additional latency.
         * <p>
         * Type:	        int
         * Default:	        1
         * Valid Values:	[0,...]
         * Importance:	    high
         */
        private final Integer fetchMinBytes;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_fetch.max.wait.ms
         * <p>
         * The maximum amount of time the server will block before answering the fetch request
         * if there isn't sufficient data to immediately satisfy the requirement given by fetch.min.bytes.
         * <p>
         * Type:	int
         * Default:	500
         * Valid Values:	[0,...]
         * Importance:	low
         */
        private final Integer fetchMaxWaitMs;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_max.partition.fetch.bytes
         * <p>
         * The maximum amount of data per-partition the server will return.
         * Records are fetched in batches by the consumer.
         * If the first record batch in the first non-empty partition of the fetch is larger than this limit,
         * the batch will still be returned to ensure that the consumer can make progress.
         * The maximum record batch size accepted by the broker is defined via message.max.bytes (broker config)
         * or max.message.bytes (topic config). See fetch.max.bytes for limiting the consumer request size.
         * <p>
         * Type:	int
         * Default:	1048576 (1 mebibyte)
         * Valid Values:	[0,...]
         * Importance:	high
         */
        private final Integer maxPartitionFetchBytes;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_max.poll.interval.ms
         * <p>
         * The maximum delay between invocations of poll() when using consumer group management.
         * This places an upper bound on the amount of time that the consumer can be idle before fetching more records.
         * If poll() is not called before expiration of this timeout, then the consumer is considered failed
         * and the group will rebalance in order to reassign the partitions to another member.
         * For consumers using a non-null group.instance.id which reach this timeout, partitions will not be
         * immediately reassigned. Instead, the consumer will stop sending heartbeats and partitions will be reassigned
         * after expiration of session.timeout.ms. This mirrors the behavior of a static consumer which has shutdown.
         * <p>
         * Type:	        int
         * Default:	        300000 (5 minutes)
         * Valid Values:	[1,...]
         * Importance:	    medium
         */
        private final Integer maxPollIntervalMs;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_max.poll.records
         * <p>
         * The maximum number of records returned in a single call to poll(). Note, that max.poll.records does not
         * impact the underlying fetching behavior. The consumer will cache the records from each fetch request
         * and returns them incrementally from each poll.
         * <p>
         * Type:	        int
         * Default:	        500
         * Valid Values:	[1,...]
         * Importance:	    medium
         */
        private final Integer maxPollRecords;

        /**
         * https://kafka.apache.org/documentation/#consumerconfigs_session.timeout.ms
         * <p>
         * The timeout used to detect client failures when using Kafka's group management facility.
         * The client sends periodic heartbeats to indicate its liveness to the broker.
         * If no heartbeats are received by the broker before the expiration of this session timeout,
         * then the broker will remove this client from the group and initiate a rebalance.
         * Note that the value must be in the allowable range as configured in the broker configuration
         * by group.min.session.timeout.ms and group.max.session.timeout.ms.
         * <p>
         * Type:	        int
         * Default:	        45000 (45 seconds)
         * Valid Values:
         * Importance:	    high
         */
        private final Integer sessionTimeoutMs;

        /**
         * 可选的,其他kafka consumer配置参数, 参考：https://kafka.apache.org/documentation/#consumerconfigs
         */
        private final Map<String, Object> props;

    }
}
