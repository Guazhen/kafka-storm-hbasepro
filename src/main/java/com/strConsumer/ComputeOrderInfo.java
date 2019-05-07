package com.strConsumer;

import com.Dao.HBaseDao;
import com.Dao.HBaseDaoImp;
import com.util.DateFmt;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  接收 order 信息处理 并存储到hbase
 *
 */
public class ComputeOrderInfo implements IRichBolt {

    Map<String, Double> HourAmtMap = null;
    Map<String, Double> DayAmtMap = null;
    long StartTime = System.currentTimeMillis();
    long StartTime2 = System.currentTimeMillis();
    HBaseDao hBaseDao = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        String tablename = "ns1:t1";
        String familycol = "f1";
        DayAmtMap = new HashMap<String,Double>();
        HourAmtMap = new HashMap<String,Double>();

        try {
            hBaseDao = new HBaseDaoImp();

        } catch (IOException e) {
            e.printStackTrace();
        }

        //查库，初始化dayMap 和 hourMap
        // 返回年月日 2018-12-21_
        String hourKey = DateFmt.getCountDate(null,DateFmt.date_short)+"_";

        //1 初始化dayMap
        // 初始化 从hbas 取数据
        List<Result> dayList = hBaseDao.getRows(tablename, familycol,"lastest", new String[]{"time","xValue","amt"});

        // 遍历数值

        for(Result rs : dayList)
        {
            for(KeyValue keyValue : rs.raw())
            {
                String rowkey = new String(rs.getRow());
                if(new String(keyValue.getQualifier()).equals("amt"))
                {
                    // 将上一次的数据加入链表中
                    DayAmtMap.put(DateFmt.getCountDate(null,DateFmt.date_short),Double.parseDouble(new String(keyValue.getValue())));
                }
            }
        }

        //2 初始化hourMap
        // 获取一天中所有小时的数据
        List<Result> hourList = hBaseDao.getRows(tablename, familycol, hourKey, new String[]{"time","xValue","amt"});
        for(Result rs : hourList)
        {
            for(KeyValue keyValue : rs.raw())
            {
                String rowkey = new String(rs.getRow());
                if(new String(keyValue.getQualifier()).equals("amt"))
                {
                    HourAmtMap.put(rowkey,Double.parseDouble(new String(keyValue.getValue())));
                }
            }
        }


    }

    public void execute(Tuple tuple) {
        String ReDate = tuple.getStringByField("date");
        String ReAmt = tuple.getStringByField("amt");
        System.out.println(ReDate);
        System.out.println(ReAmt);
        // 计算时间
        int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
        String hourStr;
        if ( hour < 10 ) {
            hourStr = "0" + hour;
        } else {
            hourStr = hour + "";
        }

        //
        // 计算金额
        String hourKey = ReDate + "_" + hourStr;
        Double dayAmt = DayAmtMap.get(ReDate);
        if ( dayAmt == null ) {
            dayAmt = 0.0;
        }

        dayAmt += Double.parseDouble(ReAmt);
        DayAmtMap.put(ReDate, dayAmt);

        Double hourAmt = HourAmtMap.get(hourKey);
        if ( hourAmt == null ) {
            hourAmt = 0.0;
        }

        hourAmt += Double.parseDouble(ReAmt);
        HourAmtMap.put(hourKey, hourAmt);

        String arr[] = DateFmt.getXValueStr();

        long currentTime = System.currentTimeMillis();

        if ( currentTime - StartTime >= 3000 ) {

            hBaseDao.insert("ns1:t1", "lastest", "f1",
                    new String[]{"time", "xValue", "amt"}, new String[]{arr[0], arr[1], DayAmtMap.get(ReDate)+""});

            hBaseDao.insert("ns1:t1", hourKey, "f1",
                    new String[]{"time", "xValue", "amt"}, new String[]{arr[0], arr[1], DayAmtMap.get(ReDate)+""});
            StartTime =System.currentTimeMillis();
        }

        if ( currentTime - StartTime2 >= 1000 * 60 ) {
            String rowkey = DateFmt.getCountDate(null,DateFmt.date_minute);// yyyyMMddHHmm
            hBaseDao.insert("ns1:t1", rowkey,"f1",
                    new String[]{"time", "xValue", "amt"},
                    new String[]{arr[0], arr[1], DayAmtMap.get(ReDate) + ""});

            StartTime2 = System.currentTimeMillis();
        }
        //

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
