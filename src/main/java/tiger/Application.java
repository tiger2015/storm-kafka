package tiger;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Application {
    public static final AnnotationConfigApplicationContext CONTEXT;

    static {
        CONTEXT = new AnnotationConfigApplicationContext(ApplicationConfig.class);
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        //BaseRichSpout kafkaSpout = new BasicKafkaSpout();
        BaseRichSpout kafkaSpout = new BatchMessageListenerKafkaSpout();
        MessageCountBolt messageCountBolt = new MessageCountBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("kafkaSpout", kafkaSpout,2);
        topologyBuilder.setBolt(MessageCountBolt.class.getSimpleName(), messageCountBolt, 4)
                .partialKeyGrouping("kafkaSpout", new Fields("cors"));
        Config config = new Config();
        config.setNumWorkers(2);
        StormSubmitter.submitTopology("message-count", config, topologyBuilder.createTopology());
        // 本地方式运行
       // LocalCluster cluster = new LocalCluster();
       // cluster.submitTopology("message-count", config, topologyBuilder.createTopology());
    }

}
