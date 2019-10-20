package tiger.bolt;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

@Slf4j
public class MyBolt extends BaseRichBolt {

    public static final String componentId = MyBolt.class.getSimpleName();

    private OutputCollector collector;

    private int taskId;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        taskId = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input) {
        log.info("task-" + taskId + " receive:" + input.getString(1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
