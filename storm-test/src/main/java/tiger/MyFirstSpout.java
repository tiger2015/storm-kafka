package tiger;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MyFirstSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // new Thread(new MyThread()).start();
    }

    @Override
    public void nextTuple() {
        long current = System.currentTimeMillis() / 1000L;
        for (int i = 0; i < 1; i++) {
            collector.emit(new Values(i, "first-message-" + i + "-" + current));
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "message"));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return config;
    }

    private class MyThread implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    long current = System.currentTimeMillis() / 1000L;
                    for (int i = 0; i < 1; i++) {
                        collector.emit(new Values(i, "first-message-" + i + "-" + current));
                    }
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
