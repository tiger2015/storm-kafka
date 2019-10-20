package tiger;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import tiger.bolt.MyBolt;
import tiger.firstmessagespout.FirstMessageSpout01;
import tiger.firstmessagespout.FirstMessageSpout02;
import tiger.firstmessagespout.FirstMessageSpout03;
import tiger.secondmessageSpout.SecondMessageSpout;

/**
 * @ClassName Application
 * @Description TODO
 * @Author zeng.h
 * @Date 2019/10/19 9:55
 * @Version 1.0
 **/
public class Application {


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException,
            AlreadyAliveException {


        FirstMessageSpout01 firstMessageSpout01 = new FirstMessageSpout01();

        FirstMessageSpout02 firstMessageSpout02 = new FirstMessageSpout02();

        FirstMessageSpout03 firstMessageSpout03 = new FirstMessageSpout03();

        SecondMessageSpout secondMessageSpout = new SecondMessageSpout();


        MyBolt myBolt = new MyBolt();


        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(FirstMessageSpout01.componentId, firstMessageSpout01, 1).setNumTasks(1);

        topologyBuilder.setSpout(FirstMessageSpout02.componentId, firstMessageSpout02, 1).setNumTasks(1);

        topologyBuilder.setSpout(FirstMessageSpout03.componentId, firstMessageSpout03, 1).setNumTasks(1);

        topologyBuilder.setSpout(SecondMessageSpout.componentId, secondMessageSpout, 1).setNumTasks(1);

        topologyBuilder.setBolt(MyBolt.componentId, myBolt, 4).setNumTasks(4)
                .fieldsGrouping(FirstMessageSpout01.componentId, new Fields("id"))
                .fieldsGrouping(FirstMessageSpout02.componentId, new Fields("id"))
                .fieldsGrouping(FirstMessageSpout03.componentId, new Fields("id"))
                .fieldsGrouping(SecondMessageSpout.componentId, new Fields("id"));
        Config config = new Config();
        config.setNumWorkers(3);
        StormSubmitter.submitTopology("test", config, topologyBuilder.createTopology());
    }

}
