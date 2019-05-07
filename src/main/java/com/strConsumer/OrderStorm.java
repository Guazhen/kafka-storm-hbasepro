package com.strConsumer;

import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

public class OrderStorm {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        String topic = "test" ;
        ZkHosts zkHosts = new ZkHosts("cdh3:2181");

        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic,
                "",
                "g");

        List<String> zkServers = new ArrayList<String>();

        for(String host : zkHosts.brokerZkStr.split(",")) {
            zkServers.add(host.split(":")[0]);
        }

        spoutConfig.zkServers = zkServers ;
        spoutConfig.zkPort = 2181;
        spoutConfig.socketTimeoutMs = 60 * 1000 ;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ;
        spoutConfig.startOffsetTime = OffsetRequest.LatestTime();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Initspout", new KafkaSpout(spoutConfig), 2) ;
        builder.setBolt("receiveBolt", new ReceiveOrderInfo(), 2).shuffleGrouping("Initspout");
        builder.setBolt("ResultBolt",
                new ComputeOrderInfo(), 2).fieldsGrouping("receiveBolt", new Fields("date"));
//        builder.setBolt("bolt1", new FormatBolt(), 2).shuffleGrouping("spout") ;
//        builder.setBolt("result", new ResultBolt(), 1).fieldsGrouping("bolt1",new Fields("date"));
//        select date,sum(amt) from table group by date

        Config conf = new Config();
        conf.setDebug(false) ;

//        StormSubmitter.submitTopology("orderinfo", conf, builder.createTopology());
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("orderinfo", conf, builder.createTopology());
        System.out.println("consumer");
    }
}
