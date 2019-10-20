package tiger.bolt;


import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import tiger.spout.CorsMessageKafkaSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName FilterCorsMessageBolt
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 14:08
 * @Version 1.0
 **/
@Slf4j
public class FilterCorsMessageBolt extends BaseRichBolt {

    public final String componentId = FilterCorsMessageBolt.class.getSimpleName();
    private OutputCollector collector;
    private int taskId;
    private Map<Integer, Integer> gridVrsMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
        gridVrsMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String source = input.getSourceComponent();
        if (source.equals(CorsMessageKafkaSpout.componentId)) {
            int corsId = input.getInteger(0);
            String message = input.getString(1);
            gridVrsMap.keySet().forEach(key -> {
                if (gridVrsMap.get(key) == corsId) {
                    this.collector.emit(new Values(key, corsId, taskId, message));
                }
            });
        } else {  // 网格信息
            int gridVrsId = input.getInteger(0);
            int corsId = input.getInteger(1);
            gridVrsMap.put(gridVrsId, corsId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("gridvrs_id", "id", "task_id", "message"));
    }
}
