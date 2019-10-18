package tiger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tiger.bolt.MessageCountBolt;
import tiger.spout.BatchMessageListenerKafkaSpout;

public class Application {
    public static final AnnotationConfigApplicationContext CONTEXT;

    static {
        CONTEXT = new AnnotationConfigApplicationContext(ApplicationConfig.class);
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {

        // 直接使用consumer
        //BaseRichSpout kafkaSpout = new BasicKafkaSpout();

        // 在spout中添加消息监听
        //BaseRichSpout kafkaSpout = new BatchMessageListenerKafkaSpout();

        // 使用外部API
        KafkaSpoutConfig<String, String> spoutConfig = CONTEXT.getBean(KafkaSpoutConfig.class);
        BaseRichSpout kafkaSpout = new KafkaSpout<>(spoutConfig);
        MessageCountBolt messageCountBolt = new MessageCountBolt();
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("kafkaSpout", kafkaSpout, 2).setNumTasks(4);
        topologyBuilder.setBolt(MessageCountBolt.class.getSimpleName(), messageCountBolt, 2).setNumTasks(4)
                .fieldsGrouping("kafkaSpout", new Fields("cors"));
        Config config = new Config();
        config.setNumWorkers(3);
        //config.setDebug(true);
        StormSubmitter.submitTopology("message-count", config, topologyBuilder.createTopology());
        // 本地方式运行
        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology("message-count", config, topologyBuilder.createTopology());
    }

}
