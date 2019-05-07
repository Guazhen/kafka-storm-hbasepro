package com;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.junit.Test;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;



public class TestKafkaConsumer {


//    public static void TestConsumer1(){
//        System.out.println("entering consumer");
//
//            Properties props = new Properties();
//
//            props.put("bootstrap.servers", "192.168.0.130:9092");
//            //每个消费者分配独立的组号
//            props.put("group.id", "test");
//
//            //如果value合法，则自动提交偏移量
//            props.put("enable.auto.commit", "true");
//
//            //设置多久一次更新被消费消息的偏移量
//            props.put("auto.commit.interval.ms", "1000");
//
//            //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
//            props.put("session.timeout.ms", "30000");
//
//            props.put("key.deserializer",
//                    "org.apache.kafka.common.serialization.StringDeserializer");
//            props.put("value.deserializer",
//                    "org.apache.kafka.common.serialization.StringDeserializer");
//
//            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//
//            consumer.subscribe(Collections.singletonList("test"));
//
//            System.out.println("Subscribed to topic " + "test");
//            int i = 0;
//
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(100);
//                for (ConsumerRecord<String, String> record : records)
//
//                    // print the offset,key and value for the consumer records.
//                    System.out.printf("offset = %d, key = %s, value = %s\n",
//                            record.offset(), record.key(), record.value());
//            }
//        }



        public static void TestConsumer() {
            Properties props = new Properties();
            props.put("zookeeper.connect", "192.168.0.130:2181");
            props.put("group.id", "g1");
            props.put("zookeeper.session.timeout.ms", "500");
            props.put("zookeeper.sync.time.ms", "250");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.offset.reset", "smallest");
            //创建消费者配置对象
            ConsumerConfig config = new ConsumerConfig(props);
            // Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
            Map<String, Integer> map = new HashMap<String, Integer>();
            map.put("test", new Integer(1));
            Map<String, List<KafkaStream<byte[], byte[]>>> msgs =
                    Consumer.createJavaConsumerConnector(new ConsumerConfig(props)).createMessageStreams(map);
            List<KafkaStream<byte[], byte[]>> msgList = msgs.get("test");
            for(KafkaStream<byte[],byte[]> stream : msgList){
                ConsumerIterator<byte[],byte[]> it = stream.iterator();
                while(it.hasNext()){
                    byte[] message = it.next().message();
                    System.out.println(new String(message));
                }
            }
        }

    public static void main(String[] args) {
        TestConsumer();
    }
}
