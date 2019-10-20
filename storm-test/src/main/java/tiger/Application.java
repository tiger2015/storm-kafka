package tiger;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Hello world!
 */
public class Application {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        MyFirstSpout firstSpout = new MyFirstSpout();
        MySecondSpout secondSpout = new MySecondSpout();

        MyBolt myBolt = new MyBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(MyFirstSpout.class.getSimpleName(), firstSpout, 4).setNumTasks(4);

        topologyBuilder.setSpout(MySecondSpout.class.getSimpleName(), secondSpout, 4).setNumTasks(4);

        topologyBuilder.setBolt(MyBolt.class.getSimpleName(), myBolt, 4)
                .setNumTasks(4)
                .fieldsGrouping(MyFirstSpout.class.getSimpleName(), new Fields("id"))
                .fieldsGrouping(MySecondSpout.class.getSimpleName(), new Fields("id"));

        Config config = new Config();
        config.setNumWorkers(3);
        // 全局设置一个时间间隔 但是时间小的优先级最高
        //config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        StormTopology topology = topologyBuilder.createTopology();
        StormSubmitter.submitTopology("test", config, topology);
    }
}
