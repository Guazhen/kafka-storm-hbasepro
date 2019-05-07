package com.Dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDaoImp implements HBaseDao {
    private Configuration conf = null;

    private Connection conn = null;

    Table table = null ;

    public HBaseDaoImp() throws IOException {
       conf = HBaseConfiguration.create();
       conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 根据API外部提供的借口，实现往hbase数据库中写入数据
     * @param put  需要写入的数据
     * @param tableName  需要写入表
     */
    public void save(Put put, String tableName) {
        TableName tablename1 = TableName.valueOf(tableName);
        try {
            table = conn.getTable(tablename1);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 向外部提供借口，实现向hbase数据库中写入多条数据
     * @param puts  需要写入的数据
     * @param tableName  需要写入的表名
     */
    public void save(List<Put> puts, String tableName) {
        TableName tableName1 = TableName.valueOf(tableName);

        try {
            table = conn.getTable(tableName1);
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }




    public List<Result> getRows(String tablename, String rowkeylike) {
//        Table table = null;
//        Configuration conf = HBaseConfiguration.create();
//        Connection conn = ConnectionFactory.createConnection(conf);

        TableName name = TableName.valueOf(tablename);
        List<Result> list = new ArrayList<Result>();
        try {
            table = conn.getTable(name);

//        String rowKeyLike = "154539";
            PrefixFilter filter = new PrefixFilter(rowkeylike.getBytes());

            Scan scan = new Scan();
            scan.setFilter(filter);
            /*
            		ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;

			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
            */

            // 加过滤器
            ResultScanner scannerresults = table.getScanner(scan);

//            Iterator<Result> it = rs.iterator();
//            byte[] f = Bytes.toBytes("f1");
//            byte[] idstr = Bytes.toBytes("id");
//            byte[] agestr = Bytes.toBytes("age");
//            byte[] namestr = Bytes.toBytes("callTime");


//            User userinfo = null;

            for ( Result result: scannerresults) {
                list.add(result);
            }

//            while (it.hasNext()) {
//                userinfo = new User();
//                Result r = it.next();
//                String ageres = Bytes.toString(r.getValue(f, agestr));
//                userinfo.setAge(Integer.valueOf(ageres));
//                String nameres = Bytes.toString(r.getValue(f, namestr));
//                userinfo.setName(nameres);
//
//                String idres = Bytes.toString(r.getValue(f, idstr));
//                userinfo.setIdstr(idres);
//
//                list.add(userinfo);
//            }
//            return list;
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    /**
     * 完成 获取指定字段数据的
     * @param tablename tablename
     * @param colfamily colfamily
     * @param rowkeylike rowkeylikey
     * @param cols  cols
     * @return list
     * @throws Exception e
     */
    public List<Result> getRows(String tablename, String colfamily, String rowkeylike, String cols[]) {
        TableName name = TableName.valueOf(tablename);

        List<Result> list = new ArrayList<Result>();
        try {
            Scan scan = new Scan();
            table = conn.getTable(name);

            PrefixFilter filter = new PrefixFilter(rowkeylike.getBytes());

            // 添加列族
            for( int i = 0; i < cols.length; i++ ) {
                scan.addColumn(colfamily.getBytes(), cols[i].getBytes());
            }
            // 过滤列族
            scan.setFilter(filter);

            // 扫描结果
            ResultScanner resultScanner = table.getScanner(scan);

            // 将结果添加到result里
            for (Result result: resultScanner) {
                list.add(result);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return list;
    }

    /**
     * 获取指定起始行 结束行
     * @param tablename  tablename
     * @param startRowk startrowk
     * @param endRow endrow
     * @return list
     * @throws Exception e
     */
    public List<Result> getRows(String tablename, String startRowk, String endRow) {

        TableName name = TableName.valueOf(tablename);

        try {
            table = conn.getTable(name);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Result> list = new ArrayList<Result>();
        Scan scan = new Scan();
        scan.setStartRow(startRowk.getBytes());
        scan.setStopRow(endRow.getBytes());

        ResultScanner resultScanner = null;
        try {
            resultScanner = table.getScanner(scan);
            for ( Result result: resultScanner) {
                list.add(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        return list;
    }

//    /**
//     * 实现根据表名 查询数据
//     * @param tableName 表名
//     * @param rowKeyLike 行键值
//     * @param cols 列
//     * @return list
//     */
//    public List<Result> getRows(String tableName, String rowKeyLike ,String cols[]) {
//        TableName tablename = TableName.valueOf(tableName);
//
//        try {
//            table = conn.getTable(tablename);
//            Scan scan = new Scan();
//            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
//
//            for( int i = 0;  i < cols.length; i++ ) {
//                scan.addColumn()
//            }
//
//            scan.setFilter(filter);
//
//
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return null;
//    }

    /**
     * 完成 根据指定的rowkeylike  删除指定的数据
     * @param tablename tablename
     * @param rowkeylike rowkeylike
     */
    public void DeleteRecord(String tablename, String rowkeylike) {
        TableName name = TableName.valueOf(tablename);
        List<Delete> deleteList = new ArrayList<Delete>();

        try {
            table = conn.getTable(name);
            PrefixFilter filter = new PrefixFilter(rowkeylike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);

            ResultScanner resultScanner = table.getScanner(scan);
            for ( Result result: resultScanner) {
                Delete del = new Delete(result.getRow());
                deleteList.add(del);
            }
            table.delete(deleteList);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 实现 向指定数据库中，写入指定的数据
     * @param tablename 表名
     * @param rowkey 行键值
     * @param colfamily 列族
     * @param quailifer 列
     * @param value 值
     */
    public void insert(String tablename, String rowkey, String colfamily, String quailifer, String value) {
        TableName name = TableName.valueOf(tablename);

        try {
            table = conn.getTable(name);
            Put put = new Put(rowkey.getBytes());
            put.add(colfamily.getBytes(), quailifer.getBytes(), value.getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 实现 向hbase数据库中插入多条数据
     * @param tablename tablename
     * @param rowkey rowkey
     * @param colfamily colfamily
     * @param quailifer qua
     * @param value value
     */
    public void insert(String tablename, String rowkey, String colfamily, String quailifer[], String value[]){
        TableName name = TableName.valueOf(tablename);

        try {
            table = conn.getTable(name);
            Put put = new Put(rowkey.getBytes());

            for ( int i = 0; i < quailifer.length; i++ ) {
                String quaval = quailifer[i];
                String valuea = value[i];
                put.add(colfamily.getBytes(), quaval.getBytes(), valuea.getBytes());
            }

            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void WriteBachData() throws IOException {

        HBaseDao hBaseDao = new HBaseDaoImp();
        String TableName = "ns1:t1";

        for ( int i = 0 ; i < 100; i++) {
            String idstr = "id_" + i;
            String rowkey = String.valueOf(System.currentTimeMillis());
            String age = String.valueOf(i);
            String name = "tom" +i ;
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("id"), Bytes.toBytes(idstr));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(age));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes(name));
            hBaseDao.save(put,TableName);
        }
    }


    public static void main(String[]args) throws Exception {
        HBaseDao hBaseDao = new HBaseDaoImp();

        String tablename = "ns1:t1";
        String familycol = "f1";
        String rowkeylike = "row";
        // 添加列族
//        String cols[] = {"age", "callTime"};
        String rowkey = "testinsertdata";
//        String startrowkey = "15453999242";
//        String endrowkey = "15453999243";
//        List<Result> resultList = ((HBaseDaoImp) hBaseDao).getRows(tablename, startrowkey, endrowkey);
//        for ( Result result: resultList) {
//            for(KeyValue keyValue: result.raw()) {
//             System.out.println(new String(keyValue.getValue()));
//            }
//        }
        String col[] = {"data1", "data2", "data3"};
        String colval[] = {"value1", "value2", "value3"};

        ((HBaseDaoImp) hBaseDao).insert(tablename, rowkey, familycol, col, colval);
//        ((HBaseDaoImp) hBaseDao).DeleteRecord(tablename, rowkeylike);

    }
}
