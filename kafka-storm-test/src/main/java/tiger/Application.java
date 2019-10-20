package tiger;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tiger.bolt.FilterCorsMessageBolt;
import tiger.bolt.GenGridVrsMessageBolt;
import tiger.bolt.MessageCountBolt;
import tiger.spout.BatchMessageListenerKafkaSpout;
import tiger.spout.CorsMessageKafkaSpout;
import tiger.spout.GridVrsInfoSpout;

public class Application {
    public static final AnnotationConfigApplicationContext CONTEXT;

    static {
        CONTEXT = new AnnotationConfigApplicationContext(ApplicationConfig.class);
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {

        Config config = new Config();
        config.setNumWorkers(3);
        StormSubmitter.submitTopology("message", config, getMessageGenTopology());
        // 本地方式运行
        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology("message-count", config, topologyBuilder.createTopology());
    }


    private static StormTopology getStormTopology() {
        // 直接使用consumer
        //BaseRichSpout kafkaSpout = new BasicKafkaSpout();

        // 在spout中添加消息监听
        //BaseRichSpout kafkaSpout = new BatchMessageListenerKafkaSpout();

        // 使用外部API
        KafkaSpoutConfig<String, String> spoutConfig = CONTEXT.getBean(KafkaSpoutConfig.class);
        BaseRichSpout kafkaSpout = new KafkaSpout(spoutConfig);
        MessageCountBolt messageCountBolt = new MessageCountBolt();
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("kafkaSpout", kafkaSpout, 8).setNumTasks(8);
        topologyBuilder.setBolt(MessageCountBolt.class.getSimpleName(), messageCountBolt, 4).setNumTasks(4)
                .fieldsGrouping("kafkaSpout", new Fields("cors"));
        return topologyBuilder.createTopology();

    }


    private static StormTopology getMessageGenTopology() {
        KafkaSpoutConfig<Integer, String> spoutConfig = CONTEXT.getBean(KafkaSpoutConfig.class);
        CorsMessageKafkaSpout corsMessageKafkaSpout = new CorsMessageKafkaSpout(spoutConfig);


        GridVrsInfoSpout gridVrsInfoSpout1 = new GridVrsInfoSpout(1);
        GridVrsInfoSpout gridVrsInfoSpout2 = new GridVrsInfoSpout(2);
        GridVrsInfoSpout gridVrsInfoSpout3 = new GridVrsInfoSpout(3);


        FilterCorsMessageBolt filterCorsMessageBolt = new FilterCorsMessageBolt();


        GenGridVrsMessageBolt genGridVrsMessageBolt = new GenGridVrsMessageBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(gridVrsInfoSpout1.componentId, gridVrsInfoSpout1, 1).setNumTasks(1);
        topologyBuilder.setSpout(gridVrsInfoSpout2.componentId, gridVrsInfoSpout2, 1).setNumTasks(1);
        topologyBuilder.setSpout(gridVrsInfoSpout3.componentId, gridVrsInfoSpout3, 1).setNumTasks(1);

        topologyBuilder.setSpout(corsMessageKafkaSpout.componentId, corsMessageKafkaSpout, 4).setNumTasks(4);


        topologyBuilder.setBolt(filterCorsMessageBolt.componentId, filterCorsMessageBolt, 2).setNumTasks(2)
                .fieldsGrouping(gridVrsInfoSpout1.componentId, new Fields("id"))
                .fieldsGrouping(gridVrsInfoSpout2.componentId, new Fields("id"))
                .fieldsGrouping(gridVrsInfoSpout3.componentId, new Fields("id"))
                .fieldsGrouping(corsMessageKafkaSpout.componentId, new Fields("id"));


        topologyBuilder.setBolt(genGridVrsMessageBolt.componentId, genGridVrsMessageBolt, 4).setNumTasks(4)
                .fieldsGrouping(filterCorsMessageBolt.componentId, new Fields("gridvrs_id"));

        return topologyBuilder.createTopology();
    }


}
