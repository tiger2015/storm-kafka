package tiger.secondmessageSpout;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName SecondMessageSpout
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 10:30
 * @Version 1.0
 **/
public class SecondMessageSpout extends BaseRichSpout {

    public static final String componentId = SecondMessageSpout.class.getSimpleName();

    private SpoutOutputCollector collector;
    private TopologyContext context;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;
    }

    @Override
    public void nextTuple() {
        sendMessage();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "message"));
    }

    private void sendMessage() {
        try {
            long current = System.currentTimeMillis() / 1000L;
            for (int i = 0; i < 100; i++) {
                collector.emit(new Values(i, "second-message-" + i + "-" + current));
            }
            TimeUnit.SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
