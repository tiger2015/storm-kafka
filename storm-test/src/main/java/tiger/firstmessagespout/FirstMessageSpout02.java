package tiger.firstmessagespout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FirstMessageSpout02 extends BaseRichSpout {

    public static final String componentId = FirstMessageSpout02.class.getSimpleName();

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        //new Thread(new MyThread()).start();
    }

    @Override
    public void nextTuple() {
        sendMessage();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "message"));
    }


    private class MyThread implements Runnable {


        @Override
        public void run() {
            while (true) {
                sendMessage();
            }
        }
    }

    private void sendMessage() {
        try {
            long current = System.currentTimeMillis() / 1000L;
            for (int i = 0; i < 100; i++) {
                if (i % 6 == 1 || i % 6 == 4)
                    collector.emit(new Values(i, "first-message-" + i + "-" + current));
            }
            TimeUnit.SECONDS.sleep(1L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
