package tiger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

public class Application {
    public static final AnnotationConfigApplicationContext CONTEXT;

    static {
        CONTEXT = new AnnotationConfigApplicationContext(ApplicationConfig.class);
        // CONTEXT.start();
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {
        // KafkaMessageListener bean = CONTEXT.getBean(KafkaMessageListener.class);

        KafkaBatchMessageSpout spout = new KafkaBatchMessageSpout();

        // KafkaSpout kafkaSpout = new KafkaSpout();
        MessageBolt messageBolt = new MessageBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(KafkaSpout.class.getSimpleName(), spout, 2);
        topologyBuilder.setBolt(MessageBolt.class.getSimpleName(), messageBolt, 8)
                .partialKeyGrouping(KafkaSpout.class.getSimpleName(), new Fields("cors"));
        Config config = new Config();
        config.setNumWorkers(3);
        StormSubmitter.submitTopology("message-count-test", config, topologyBuilder.createTopology());
        // 本地方式运行
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("message-count", config, topologyBuilder.createTopology());
    }

}
