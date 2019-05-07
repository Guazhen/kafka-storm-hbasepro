package com.Dao;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * 向hbase数据中写入数据，获取数据一些操作
 */

public interface HBaseDao {
    public void save(Put put, String tableName);
    public void save(List<Put> Put, String tableName) ;
    public List<Result> getRows(String tablename, String rowkeylike);
    public List<Result> getRows(String tablename, String colfamily, String rowkeylike, String cols[]);
    public List<Result> getRows(String tablename, String startRowk, String endRow);
//    public List<Result> getRows(String tableName, String rowKeyLike ,String cols[]);
    public void DeleteRecord(String tablename, String rowkeylike);
    public void insert(String tablename, String rowkey, String colfamily, String quailifer, String value);
    public void insert(String tablename, String rowkey, String colfamily, String quailifer[], String value[]);
}
