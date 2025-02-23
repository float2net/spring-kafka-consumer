package float2net.framework.springkafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

/**
 * kafka consumer service, String type
 */
@Service
@Slf4j
public class StringKafkaConsumerService extends KafkaConsumerServiceBase<String, String> {
    public StringKafkaConsumerService(KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        super("org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer",
                kafkaListenerEndpointRegistry);
    }
}
