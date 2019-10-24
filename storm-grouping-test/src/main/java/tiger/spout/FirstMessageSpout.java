package tiger.spout;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import tiger.Application;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FirstMessageSpout extends BaseRichSpout {
    private int maxId;
    private int minId;
    private SpoutOutputCollector collector;
    public final String componentId;

    public FirstMessageSpout(String componentId, int minId, int maxId) {
        this.maxId = maxId;
        this.minId = minId;
        this.componentId = componentId;
    }

    public FirstMessageSpout(int minId, int maxId) {
        this(FirstMessageSpout.class.getSimpleName(), minId, maxId);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

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
        long current = System.currentTimeMillis() / 1000L;
        for (int i = minId; i <= maxId; i++) {
            collector.emit(new Values(i, "first-message-" + i + "-" + Application.INDEX.getAndIncrement()));
        }
        try {
            TimeUnit.SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
