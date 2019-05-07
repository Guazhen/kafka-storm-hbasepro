package com;


import com.Dao.HBaseDao;
import com.Dao.HBaseDaoImp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.util.PropertiesUtil;


import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class test {


    public static void TestRand() throws InterruptedException {
        Random random = new Random();

        String[] order_area = {"1", "2", "3", "4", "5", "6", "7", "8"};

        while ( true) {
            int r = random.nextInt();
            double d = random.nextFloat();
            d = d * 1000;
            BigDecimal bigDecimal = new BigDecimal(d);
            d = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = simpleDateFormat.format(new Date());

            System.out.println(random.nextInt(100));

            String area = order_area[random.nextInt(8)];
            System.out.println(area);
            System.out.println(date);
            System.out.println(r);
            System.out.println(d);
            Thread.sleep(1000);
        }
    }

    public static void TestKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.130:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("broker-list", "192.168.0.130:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "test";

        Producer<String, String> procuder = new KafkaProducer<String,String>(props);

        for (int i = 1; i <= 10; i++) {
            String value = "value_" + i;
            System.out.println(value);
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, value);

            procuder.send(msg);
            Thread.sleep(1000);
        }
    }

    public static void TestHbaseWrite() throws IOException {
        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = null ;

        // 获取表名  ns1:calllogs
        TableName name = TableName.valueOf("ns1:calllogs");
        table = conn.getTable(name);
        // 序列的格式化编号

        String rowkey = "testdata";
        String caller = "value1";

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("caller"), Bytes.toBytes(caller));

        table.put(put);

    }

    public static void TestHbaseRead() throws IOException {
        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = null ;

        // 获取表名  ns1:calllogs
        TableName name = TableName.valueOf("ns1:t1");
        table = conn.getTable(name);

        byte[] rowid = Bytes.toBytes("testdata");
        Get get = new Get(Bytes.toBytes("testdata"));
        Result result = table.get(get);

        byte[] idvalue = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("caller"));

        System.out.println(Bytes.toString(idvalue));

    }

    /**
     *
     *  Configuration conf = HBaseConfiguration.create();
     *             Connection conn = ConnectionFactory.createConnection(conf);
     *             TableName name = TableName.valueOf("ns1:calllogs");
     *             table = conn.getTable(name);
     *
     *
     *                    List<CallLog> list = new ArrayList<CallLog>();
     *         try {
     *             Scan scan = new Scan();
     *             ResultScanner rs = table.getScanner(scan);
     *             Iterator<Result> it = rs.iterator();
     *             byte[] f = Bytes.toBytes("f1");
     *
     *             byte[] caller = Bytes.toBytes("caller");
     *             byte[] callee = Bytes.toBytes("callee");
     *             byte[] callTime = Bytes.toBytes("callTime");
     *             byte[] callDuration = Bytes.toBytes("callDuration");
     *
     *             CallLog log = null ;
     *
     *             while(it.hasNext()){
     *                 log = new CallLog();
     *                 Result r = it.next();
     *                 //TODO 设置用户名
     *                 String callerStr = Bytes.toString(r.getValue(f, caller)) ;
     *                 log.setCaller(callerStr);
     *                 log.setCallerName(ps.selectNameByPhone(callerStr));
     *
     *                 //TODO 设置用户名
     *                 String calleeStr = Bytes.toString(r.getValue(f, callee)) ;
     *                 log.setCallee(calleeStr);
     *                 log.setCalleeName(ps.selectNameByPhone(calleeStr));
     *
     *                 log.setCallTime(Bytes.toString(r.getValue(f, callTime)));
     *                 log.setCallDuration(Bytes.toString(r.getValue(f, callDuration)));
     *                 list.add(log);
     *             }
     *
     *             return list ;
     *
     */

    public static void GetRows() throws IOException {





    }
    public static void main(String []args) throws Exception {

        System.out.println("start test");
//        TestHbaseRead();
        TestHbaseWrite();
//        HBaseDao hBaseDao = new HBaseDaoImp();

        //
//        hBaseDao.insert("ns1:t1", "20189012", "f1",
//                new String[]{"Ak","Bk"}, new String[]{"Avalue", "Bvalue"});

//        TestKafka();
//        TestHbaseWrite();
//        TestHbaseRead();
//        HBaseDao hBaseDao = new HBaseDaoImp();
//        List<Put> putList = null;
//        String rowkey = "TestDao";
//        String caller = "DataDao";
//
//        Put put = new Put(Bytes.toBytes(rowkey));
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("testdata"), Bytes.toBytes(caller));
//
//        putList.add(put);
//
//        rowkey = "testDao1";
//        caller = "Data111";
//        put = new Put(Bytes.toBytes(rowkey));
//        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("testdata22"), Bytes.toBytes(caller));
//
//        putList.add(put);


//        hBaseDao.save(putList, "ns1:t1");
    }


}
