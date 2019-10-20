package tiger;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import tiger.bolt.MyBolt;
import tiger.spout.FirstMessageSpout;
import tiger.spout.SecondMessageSpout;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @ClassName Application
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 9:55
 * @Version 1.0
 **/
public class Application {

    public static final AtomicLong INDEX = new AtomicLong(0);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {


        FirstMessageSpout firstMessageSpout01 = new FirstMessageSpout("firsMessageSpout-1", 1, 10);
        FirstMessageSpout firstMessageSpout02 = new FirstMessageSpout("firsMessageSpout-2", 11, 20);
        FirstMessageSpout firstMessageSpout03 = new FirstMessageSpout("firsMessageSpout-3", 21, 30);

        SecondMessageSpout secondMessageSpout01 = new SecondMessageSpout("secondMessageSpout-1", 1, 15);
        SecondMessageSpout secondMessageSpout02 = new SecondMessageSpout("secondMessageSpout-2", 16, 31);

        MyBolt myBolt = new MyBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(firstMessageSpout01.componentId, firstMessageSpout01, 1).setNumTasks(1);
        topologyBuilder.setSpout(firstMessageSpout02.componentId, firstMessageSpout02, 1).setNumTasks(1);
        topologyBuilder.setSpout(firstMessageSpout03.componentId, firstMessageSpout03, 1).setNumTasks(1);

        topologyBuilder.setSpout(secondMessageSpout01.componentId, secondMessageSpout01, 1).setNumTasks(1);
        topologyBuilder.setSpout(secondMessageSpout02.componentId, secondMessageSpout02, 1).setNumTasks(1);

        topologyBuilder.setBolt(MyBolt.componentId, myBolt, 4).setNumTasks(4)
                .fieldsGrouping(firstMessageSpout01.componentId, new Fields("id"))
                .fieldsGrouping(firstMessageSpout02.componentId, new Fields("id"))
                .fieldsGrouping(firstMessageSpout03.componentId, new Fields("id"))
                .fieldsGrouping(secondMessageSpout01.componentId, new Fields("id"))
                .fieldsGrouping(secondMessageSpout02.componentId, new Fields("id"));
        Config config = new Config();
        config.setNumWorkers(3);
        StormSubmitter.submitTopology("test", config, topologyBuilder.createTopology());
    }
}
