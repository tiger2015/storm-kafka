package tiger.bolt;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @ClassName GenGridVrsMessageBolt
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 14:13
 * @Version 1.0
 **/
@Slf4j
public class GenGridVrsMessageBolt extends BaseRichBolt {

    public final String componentId = GenGridVrsMessageBolt.class.getSimpleName();

    private OutputCollector collector;

    private int taskId;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        taskId = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input) {
        int gridVrsId = input.getInteger(0);
        int corsId = input.getInteger(1);
        int taskId = input.getInteger(2);
        String message = input.getString(3);
        log.info(String.format("gridvrs:%d, cors:%d, source task:%d, task:%d, message:%s", gridVrsId, corsId, taskId,
                this.taskId, message));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
