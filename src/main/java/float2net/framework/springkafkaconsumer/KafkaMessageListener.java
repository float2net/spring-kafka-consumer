package float2net.framework.springkafkaconsumer;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.GenericMessageListener;
import org.springframework.util.Assert;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * kafka消息监听器（每个注册的kafka消费任务都有对应这个类的一个实例）
 * 对消息进行基本校验，限流、反压、多线程分发处理。
 */
@Slf4j
public class KafkaMessageListener<K, V> implements GenericMessageListener<ConsumerRecords<K, V>> {


    //    private static ThreadLocal<Long> recvCount = ThreadLocal.withInitial(() -> 0L);  //统计的量
    private static final AtomicLong recvCount = new AtomicLong(0L);  //统计的量
    private static final AtomicLong recvSec = new AtomicLong(System.currentTimeMillis() / 1000);  //统计的秒

    /**
     * 抽象消息处理任务
     */
    private AbstractMessageProcessTask processTask;

    /**
     * 每秒限流记录数
     */
    private int limit;

    /**
     * 限流器
     */
    private RateLimiter rateLimiter;

    /**
     * 任务队列
     */
    private ThreadPoolExecutor taskExecutor;

    /**
     * 处理消息线程的并发数，也就是线程池队列长度
     */
    private Integer processorConcurrency;

    /**
     * 构造器， 初始化多线程任务队列和限流器
     * <p>
     * 线程池使用说明参考： https://www.baeldung.com/java-rejectedexecutionhandler
     *
     * @param processTask
     */
    public KafkaMessageListener(AbstractMessageProcessTask<K, V> processTask) {
        this.processTask = processTask;
        this.limit = Optional.ofNullable(processTask.config.getRateLimit()).filter(n -> n > 0).orElse(0);
        if (this.limit > 0) {
            rateLimiter = RateLimiter.create(limit);
        }
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("float2net-kafka-processor-" + processTask.getTaskId() + "-%d")
                .setDaemon(false).setPriority(Thread.NORM_PRIORITY).build();
        this.processorConcurrency = Optional.ofNullable(processTask.config.getProcessorConcurrency()).filter(n -> n > 0).orElse(10);
        /**
         * coreSize和maxPoolSize都设置为处理并发度
         */
        int coreSize = this.processorConcurrency;
        int maxPoolSize = this.processorConcurrency;
        /**
         * 设置一个恰当的队列长度，以尽量保证在CallerRunsPolicy策略下当由分发主线程执行消费任务时，
         * 有足够的时间在队列长度消耗完之前，完成消费任务并继续拉取kafka数据进行分发。
         * 如果这个队列长度过小，有可能poolExecutor中任务已经处理完毕，都在等待分发主线程提交消费任务
         * 如果这个队列长度过大，会占用一定内存资源，当程序重启时也会导致队列中中数据丢失。
         * 当前设置为与coreSize同等长度，也就是假设分发主线程能够在平均两个任务处理时间内完成一个任务的处理。
         */
        int queueCapacity = this.processorConcurrency;
        /**
         * 线程池中线程完成任务后处于idle状态（比如队列暂时为空，没有新的任务接收）等待被销毁的时间。
         * 该值如果设置太短，则线程被销毁太频繁，重建线程会占用资源，从而降低线程池的使用效率.
         */
        int keepAliveTime = 100;
        /**
         * CallerRunsPolicy 保证当任务超出队列长度时，由分发主线程来处理任务，因而暂停了kafka消费，形成反压的效果。
         * 改成使用自定义的SleepPolicy,当任务队列饱和时，通过休眠消费线程的方式实现反压的效果。
         */
        this.taskExecutor = new ThreadPoolExecutor(coreSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity), threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()/*new SleepPolicy()*/);
    }

    /**
     * 内部的KAFKA消息处理方法（包含限流，反压，多线程消费处理）
     *
     * @param records
     */
    @Override
    public void onMessage(ConsumerRecords<K, V> records) {

        //校验
        Assert.isTrue(records != null && !records.isEmpty(), "kafka记录不能为空");

        if (log.isDebugEnabled()) {
            log.debug("接收{}条消息({})", records.count(), String.format("%08X", records.hashCode()));
        }

        //>>>统计秒计数
        if (log.isTraceEnabled()) {
            final long sec = System.currentTimeMillis() / 1000;
            if (sec != recvSec.get()) {
                log.trace("前{}秒共接收到{}条消息", sec - recvSec.get(), recvCount.get());
                recvCount.set(records.count());
                recvSec.set(sec);
            } else {
                recvCount.addAndGet(records.count());
            }
        }
        //<<<

        //限流
        if (limit > 0) {
            long l1 = System.currentTimeMillis();
            rateLimiter.acquire(1);
            long l2 = System.currentTimeMillis();
            if (log.isWarnEnabled()) {
                if ((l2 - l1) > 1000) {
                    log.warn("kafka消费：等待限流锁'{}'超过了1秒", rateLimiter.getRate());
                }
            }
        }

        //多线程任务分发
        if (log.isDebugEnabled()) {
            log.debug("分发{}条消息({})", records.count(), String.format("%08X", records.hashCode()));
        }
        taskExecutor.execute(() -> processTask.accept(records));
    }

    /**
     * 自定义的队列饱和处理策略：
     * 队列饱和时，休眠一下下，再重新提交任务，
     * 如果队列仍然饱和，则会继续休眠，再重新提交任务。。。
     * 直到队列有空间时任务提交成功。
     * 在队列饱和情况下的等待，暂缓了消费kafka，形成了反压。
     */
    private static class SleepPolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                log.trace("sleeping...");
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                log.warn("Sleep-Policy thread sleep interrupted");
                Thread.currentThread().interrupt();
            }

            //重新提交任务 //FIXME 这里有stackOverFlow可能！！！
            executor.execute(r);
        }
    }
}
