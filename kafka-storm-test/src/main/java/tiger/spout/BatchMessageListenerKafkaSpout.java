package tiger.spout;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerSeekAware;
import tiger.Application;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于kafka消息监听的spout
 *
 * @ClassName BatchMessageListenerKafkaSpout
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/10 16:13
 * @Version 1.0
 **/
@Slf4j
public class BatchMessageListenerKafkaSpout extends BaseRichSpout implements BatchMessageListener<String, String>,
        ConsumerSeekAware {
    private static AtomicLong counter = new AtomicLong(0);
    private TopologyContext context;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        ConcurrentMessageListenerContainer<String, String> container = Application.CONTEXT.getBean("container",
                ConcurrentMessageListenerContainer.class);
        container.getContainerProperties().setMessageListener(this);
        container.start();
        log.info("init kafkaBatchMessageSpout");
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cors", "message_key"));
    }


    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data) {
        for (ConsumerRecord record : data) {
            log.info("receive record:" + record.key()+", "+record.value());
            this.collector.emit(new Values(record.key(), record.value()));
        }
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {

    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.info("seek partition offset to end");
        assignments.forEach((key, value) -> callback.seekToEnd(key.topic(), key.partition()));
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {

    }
}
