package float2net.framework.springkafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * byte[]类型的消费任务，提供两种消费方式：
 * 1, 批量消费，重载process(ConsumerRecords）
 * 2, 逐个消费，重载process(ConsumerRecord）
 */
@Slf4j
public class BytesMessageProcessTask extends AbstractMessageProcessTask<byte[], byte[]> {
    public BytesMessageProcessTask(String taskId, Config config) {
        super(taskId, config);
    }

    /**
     * 如果要批量消费记录，则重载这个方法。
     *
     * @param records 待处理的一批kafka消息
     */
    @Override
    protected void process(ConsumerRecords<byte[], byte[]> records) {
        final Iterable<ConsumerRecord<byte[], byte[]>> iterable = () -> records.iterator();
        final Stream<ConsumerRecord<byte[], byte[]>> stream = StreamSupport.stream(iterable.spliterator(), false);
        stream.forEach(record -> process(record));
    }

    /**
     * 如果要逐个消费记录，则重载这个方法。
     *
     * @param record 待处理的一条kafka消息
     */
    protected void process(ConsumerRecord<byte[], byte[]> record) {
        throw new UnsupportedOperationException("kafka记录消费方法暂未实现");
    }
}
