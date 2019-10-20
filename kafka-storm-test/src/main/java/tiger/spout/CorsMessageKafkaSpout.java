package tiger.spout;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

/**
 * @ClassName CorsMessageKafkaSpout
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 13:47
 * @Version 1.0
 **/
public class CorsMessageKafkaSpout extends KafkaSpout<Integer, String> {
    public static final String componentId = CorsMessageKafkaSpout.class.getSimpleName();

    public CorsMessageKafkaSpout(KafkaSpoutConfig<Integer, String> kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
    }
}
