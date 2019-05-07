package com.strConsumer;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Kafka消费者
 */
public class ReceiveOrderInfo implements IRichBolt {
    OutputCollector collector = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple orderinfo) {
        String msg = orderinfo.getString(0);
        System.out.println("receive msg: " + msg);

        // 解析 信息
        String[]orinfo = msg.split(",");
        System.out.println(orinfo.length);

        String date = orinfo[2];
        String amt = orinfo[1];

        this.collector.emit(new Values(date, amt));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("date", "amt"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
