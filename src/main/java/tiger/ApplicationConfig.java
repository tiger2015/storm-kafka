package tiger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.*;

import java.time.Duration;
import java.util.*;

@Configuration
@PropertySource(value = {"classpath:application.properties"})
@ComponentScan(basePackages = {"tiger"})
@EnableKafka
public class ApplicationConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.max.poll.records}")
    private int maxPollRecords;

    @Value("${kafka.consumer.enable.auto.commit}")
    private boolean enableAutoCommit;

    @Value("${kafka.consumer.auto.commit.interval.ms}")
    private int autoCommitIntervalMs;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.key.deserializer}")
    private String keyDeserializerClass;

    @Value("${kafka.consumer.value.deserializer}")
    private String valueDeserializerClass;

    @Value("${kafka.consumer.topics}")
    private String[] topics;

    @Value("${kafka.consumer.concurrency}")
    private int concurrency;

    @Value("${kafka.consumer.batch-listener}")
    private boolean batchListener;


    @Value("${kafka.producer.key.serializer}")
    private String keySerializer;

    @Value("${kafka.producer.value.serializer}")
    private String valueSerializer;


    private Map<String, Object> consumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        return props;
    }

    private Map<String, Object> producerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return props;
    }

    private ConsumerFactory consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    public Consumer kafkaConsumer() {
        Consumer consumer = consumerFactory().createConsumer();
       // consumer.subscribe(Arrays.asList(topics));
        Map<String, List<PartitionInfo>> map = consumer.listTopics();
        for (String topic : map.keySet()) {
            List<PartitionInfo> partitionInfos = map.get(topic);
            if (!"test".equals(topic)) {
                continue;
            }
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
            consumer.assign(topicPartitions);
            consumer.seekToEnd(topicPartitions);
        }
        return consumer;
    }

    public ProducerFactory producerFactory() {
        return new DefaultKafkaProducerFactory(producerConfig());
    }

    @Bean
    public KafkaTemplate kafkaTemplate() {
        return new KafkaTemplate(producerFactory());
    }

}
