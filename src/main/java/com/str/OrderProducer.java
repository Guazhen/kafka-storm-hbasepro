package com.str;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

// kafka 生产者

/**
 * 1。生成日记信息到Kafka中
 * 2。简历kafka信息
 *
 */
public class OrderProducer extends Thread{

    private final String topic ;
    private final Properties properties = new Properties();


    private final Producer<String, String> producer;

//    producer.send(new ProducerRecord<String, String>("calllog", Integer.toString(i), Integer.toString(i)));

//    ProducerConfig producerConfig;

    public OrderProducer(String topic){

        properties.put("bootstrap.servers", "192.168.0.130:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        producerConfig = new ProducerConfig(properties);

        producer = new KafkaProducer<String, String>(properties);

        this.topic = topic;
    }

    @Override
    public void run() {
        //// order_id,order_amt,create_time,area_id,user_id
//        super.run();

        Random random = new Random();
        Long order_id;
        double order_amt;
        BigDecimal bigDecimal;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String order_create_time;
        String []order_area = {"1", "2", "3", "4", "5", "6", "7", "8"};
        while(true) {
            order_id = random.nextLong();
            if ( order_id < 0 ){
                order_id = order_id / (-1);
            }
            order_id = order_id % KafkaPro.ORDER_ID;
            order_amt = random.nextDouble() * 1000;
            bigDecimal = new BigDecimal(order_amt);
            order_amt = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

            order_create_time = simpleDateFormat.format(new Date());

            String message = order_id +"," + order_amt + "," + order_create_time + "," + order_area[random.nextInt(8)]
                        +"," + random.nextInt(100);

            producer.send(new ProducerRecord<String, String>("test", Integer.toString(1), message));

            System.out.println("message: " + message);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String []args){
        OrderProducer orderProducer = new OrderProducer(KafkaPro.topic);
        orderProducer.start();

    }
}
