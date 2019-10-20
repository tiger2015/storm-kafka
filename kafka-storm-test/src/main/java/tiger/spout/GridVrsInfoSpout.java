package tiger.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName GridVrsInfoSpout
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 13:55
 * @Version 1.0
 **/
public class GridVrsInfoSpout extends BaseRichSpout {

    public final String componentId;

    private SpoutOutputCollector collector;
    private int id;

    private Map<Integer, Integer> gridVrsMap;

    public GridVrsInfoSpout(int id) {
        componentId = GridVrsInfoSpout.class.getSimpleName() + "-" + id;
        this.id = id;
        gridVrsMap = new HashMap<>();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            if (i % 10 == id) {
                gridVrsMap.putIfAbsent(i, random.nextInt(20) + 1);
            }
        }
    }

    @Override
    public void nextTuple() {
        sendMessage();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("gridvrs_id", "id"));
    }


    private void sendMessage() {
        gridVrsMap.forEach((key, value) -> this.collector.emit(new Values(key, value)));
        try {
            TimeUnit.SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
