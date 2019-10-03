package tiger;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MessageBolt extends BaseRichBolt {
    private static AtomicLong counter = new AtomicLong(0);
    private TopologyContext context;
    private OutputCollector collector;
    private KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.collector = collector;
        kafkaTemplate = Application.CONTEXT.getBean(KafkaTemplate.class, "kafkaTemplate");
    }

    @Override
    public void execute(Tuple input) {
        String corsId = input.getString(0);
        String messageKey = input.getString(1);
        log.info("message:" + corsId + "-" + messageKey);
        long count = counter.incrementAndGet();
        log.info("message count:" + count);
        kafkaTemplate.send("result", "message count", count + "");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
