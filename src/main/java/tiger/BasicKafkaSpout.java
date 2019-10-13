package tiger;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.time.Duration;
import java.util.Map;

/**
 * 基于单消费者处理kafka消息
 */

@Slf4j
public class BasicKafkaSpout extends BaseRichSpout {
    private TopologyContext context;
    private SpoutOutputCollector collector;
    private Consumer consumer;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        consumer = Application.CONTEXT.getBean("kafkaConsumer", Consumer.class);
    }

    @Override
    public void nextTuple() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord record : records) {
            this.collector.emit(new Values(record.key(), record.value()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("cors", "message_key"));
    }
}
