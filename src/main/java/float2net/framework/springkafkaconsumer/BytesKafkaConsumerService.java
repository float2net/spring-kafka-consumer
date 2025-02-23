package float2net.framework.springkafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

/**
 * kafka consumer service, byte[] type
 */
@Service
@Slf4j
public class BytesKafkaConsumerService extends KafkaConsumerServiceBase<byte[], byte[]> {
    public BytesKafkaConsumerService(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        super("org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                kafkaListenerEndpointRegistry);
    }
}
