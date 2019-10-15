package tiger.bolt;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import tiger.Application;
import tiger.service.KafkaProducerService;
import tiger.service.KafkaProducerServiceImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MessageCountBolt extends BaseRichBolt {
    private static Map<String, AtomicLong> counterMap;
    private TopologyContext context;
    private OutputCollector collector;

    private KafkaProducerService<String, String> kafkaProducerService;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.collector = collector;
        counterMap = new ConcurrentHashMap<>();
        kafkaProducerService = Application.CONTEXT.getBean(KafkaProducerServiceImpl.class);
    }

    @Override
    public void execute(Tuple input) {
        String corsId = input.getString(0);
        //String messageKey = input.getString(1);
        counterMap.putIfAbsent(corsId, new AtomicLong(0));
        long count = counterMap.get(corsId).addAndGet(1);
        //log.info("message count: " + corsId + ":" + count);
        if (corsId.equals("8")) {
            log.info("count:" + corsId + "," + count);
            kafkaProducerService.sendMessage("result", corsId, count + "");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
