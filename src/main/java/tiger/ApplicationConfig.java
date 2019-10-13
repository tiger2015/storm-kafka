package tiger;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.*;

@Configuration
@PropertySource(value = {"classpath:application.properties"})
@ComponentScan(basePackages = {"tiger"})
@EnableKafka
@Slf4j
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

    @Value("${kafka.consumer.time-offset.ms}")
    private long timeOffsetInMills;

    @Value("${kafka.consumer.poll.timeout.ms}")
    private long pollTimeout;

    @Bean
    public Map<String, Object> consumerConfig() {
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

    @Bean
    public ConsumerFactory consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    @Profile("consumer")
    public Consumer kafkaConsumer() {
        Consumer consumer = consumerFactory().createConsumer();
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (String topic : topics) {
            List<PartitionInfo> list = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : list) {
                log.info("partition info: " + partitionInfo.toString());
                topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
        }
        // 分配分区
        consumer.assign(topicPartitions);
        // 指定到不同的时间戳进行消费
        Map<TopicPartition, Long> map = new HashMap<>();
        topicPartitions.forEach(partition -> map.put(partition, System.currentTimeMillis() - timeOffsetInMills));
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(map);
        offsetsForTimes.forEach((key, value) -> {
            if (value == null) {
                consumer.seekToEnd(Arrays.asList(key));
            } else {
                consumer.seek(key, value.offset());
            }
        });
        return consumer;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory listenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory containerFactory = new ConcurrentKafkaListenerContainerFactory();
        containerFactory.setConcurrency(concurrency);
        containerFactory.setBatchListener(batchListener);
        containerFactory.setConsumerFactory(consumerFactory());
        containerFactory.afterPropertiesSet();
        return containerFactory;
    }

    @Bean
    public ConcurrentMessageListenerContainer container() {
        ContainerProperties containerProperties = new ContainerProperties(topics);
        containerProperties.setPollTimeout(pollTimeout);
        ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory(),
                containerProperties);
        container.setConcurrency(concurrency);
        container.setAutoStartup(false); // 不自动启动，不然listener找不到报错
        return container;
    }
}
