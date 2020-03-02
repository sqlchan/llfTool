package com.sql.hbase.nosleep.hbase;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HbaseCoprocessorTest extends BaseRegionObserver {

    static Configuration conf = HBaseConfiguration.create();
    static Connection conn = null;
    static Table table = null;
    static {
        conf.set("hbase.zookeeper.quorum", "hdh21:2181,hdh22:2181,hdh23:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf("XFUSION:HIK_GAIT_INDEX_INFO"));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        Get get ;
        get = new Get(Bytes.toBytes("202001010000d2a7561935c108c906051320000"));
        get.addFamily(Bytes.toBytes("info"));
        Result results = table.get(get);
        String row = Bytes.toString(results.getRow());
        byte[] model = results.getValue(Bytes.toBytes("info"), Bytes.toBytes("model"));
        String ss = Base64.encodeBase64String(model);
        System.out.println(ss);
//        Put put = new Put(Bytes.toBytes("2"));
//        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("h1"),Bytes.toBytes("11"));
//        table.put(put);
//
//        Append append = new Append(Bytes.toBytes("2"));
//        append.add(Bytes.toBytes("f1"),Bytes.toBytes("h1"),Bytes.toBytes("22"));
//        table.append(append);
        table.close();
        conn.close();
    }

    /**
     * 此方法是在真正的put方法调用之前进行调用
     * 参数put为table.put(put)里面的参数put对象，是要进行插入的那条数据
     *
     * 例如：要向关注表里面插入一条数据    姓名：二狗子    关注的明星：王宝强
     * shell语句：put 'guanzhu','ergouzi', 'cf:star', 'wangbaoqiang'
     *
     * */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        table.put(put);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        conn.close();
    }
}
