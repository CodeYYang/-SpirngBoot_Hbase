package org.spring.springboot.service;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.logging.Logger;

public class HbaseClient {
    private static Logger logger = Logger.getLogger(HbaseClient.class.getName());

    private static Connection conn = null;

    static{
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "bigdata01,bigdata02,bigdata03,bigdata04");
        conf.set("hadoop.user.name","hadoop");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] get(TableName tab, Get get) {
        Table table = null;
        Result result = null;
        try {
            table = conn.getTable(tab);
            result = table.get(get);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.value();
    }

    public byte[] get(TableName tab, byte[] rowkey, byte[] cf, byte[] cn) {
        Table table = null;
        Result result = null;
        try {
            table = conn.getTable(tab);
            Get get = new Get(rowkey);
            get.addColumn(cf, cn);
            result = table.get(get);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result.getValue(cf, cn);
    }

    public void put(TableName tab, byte[] rowkey, byte[] cf, byte[] cn, byte[] val) {
        Table table = null;
        Result result = null;
        try {
            table = conn.getTable(tab);
            Put put = new Put(rowkey);
            put.addColumn(cf, cn, val);
            table.put(put);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String tab, Put put) {
        Table table = null;
        Result result = null;
        try {
            table = conn.getTable(TableName.valueOf(tab));
            table.put(put);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String tab, List<Put> puts) {
        Table table = null;
        Result result = null;
        try {
            table = conn.getTable(TableName.valueOf(tab));
            table.put(puts);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** --------------------------------------------------------------------*/


//    public CourseFeature getCourseFeature(String courseId) {
//        Table table = null;
//        Result result = null;
//
//        try {
//            byte[] rowkey = Bytes.toBytes(courseId);
//            table = conn.getTable(TableName.valueOf("course"));
//            Get get = new Get(rowkey);
//            result = table.get(get);
//            table.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        byte[] registercnt = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("registercnt"));
//        byte[] quitcnt = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("quitcnt"));
//        byte[] videocnt = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("videocnt"));
//        byte[] duration = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("videoduration"));
//
//        return new CourseFeature(
//                Bytes.toDouble(registercnt),
//                Bytes.toDouble(quitcnt),
//                Bytes.toDouble(videocnt),
//                Bytes.toDouble(duration));
//    }


    public Double getProb(String feature, String kind, String statistic) {
        Table table = null;
        Result result = null;

        try {
            byte[] rowkey = Bytes.toBytes(feature);
            table = conn.getTable(TableName.valueOf("bayes"));
            Get get = new Get(rowkey);
            result = table.get(get);
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        byte[] value = result.getValue(Bytes.toBytes(kind), Bytes.toBytes(statistic));
        return Bytes.toDouble(value);
    }



//    public RecordRow getRecord(String courseId, String stuId) {
//        Table table = null;
//        Result result = null;
//
//        try {
//            byte[] rowkey = Bytes.toBytes(courseId+stuId);
//            table = conn.getTable(TableName.valueOf("student"));
//            Get get = new Get(rowkey);
//            result = table.get(get);
//            table.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        byte[] coursecnt_stu = result.getValue(Bytes.toBytes("student"), Bytes.toBytes("coursecnt"));
//        byte[] localwatchingtime_stu = result.getValue(Bytes.toBytes("student"), Bytes.toBytes("localwatchingtime"));
//        byte[] rate_stu = result.getValue(Bytes.toBytes("student"), Bytes.toBytes("rate"));
//        byte[] videoduration_stu = result.getValue(Bytes.toBytes("student"), Bytes.toBytes("videoduration"));
//        byte[] videoprogresstime_stu = result.getValue(Bytes.toBytes("student"), Bytes.toBytes("videoprogresstime"));
//        byte[] watchingcount_stu = result.getValue(Bytes.toBytes("student"), Bytes.toBytes("watchingcount"));
//
//        byte[] registercnt_cs = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("registercnt"));
//        byte[] quitcnt_cs = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("quitcnt"));
//        byte[] videocnt_cs = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("videocnt"));
//        byte[] videoduration_cs = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("videoduration"));
//
//        byte[] localwatchingtime_stucs = result.getValue(Bytes.toBytes("stucs"), Bytes.toBytes("localwatchingtime"));
//        byte[] videoprogresstime_stucs = result.getValue(Bytes.toBytes("stucs"), Bytes.toBytes("videoprogresstime"));
//        byte[] watchingcount_stucs = result.getValue(Bytes.toBytes("stucs"), Bytes.toBytes("watchingcount"));
//
//        byte[] drop = result.getValue(Bytes.toBytes("stucs"), Bytes.toBytes("drop"));
//
//        return new RecordRow(
//                Bytes.toDouble(rate_stu),
//                Bytes.toDouble(watchingcount_stu),
//                Bytes.toDouble(videoduration_stu),
//                Bytes.toDouble(localwatchingtime_stu),
//                Bytes.toDouble(videoprogresstime_stu),
//                Bytes.toDouble(coursecnt_stu),
//                Bytes.toDouble(registercnt_cs),
//                Bytes.toDouble(quitcnt_cs),
//                Bytes.toDouble(videocnt_cs),
//                Bytes.toDouble(videoduration_cs),
//                Bytes.toDouble(watchingcount_stucs),
//                Bytes.toDouble(localwatchingtime_stucs),
//                Bytes.toDouble(videoprogresstime_stucs),
//                Bytes.toInt(drop) == 1
//        );
//    }
}
