package com.sql.hbase.nosleep.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTest {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "10.33.57.46");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("llf"));
//        Get get = new Get(Bytes.toBytes("1"));
//        get.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("h1"));
//        Result result1 = table.get(get);
//        byte[] name = result1.getValue(Bytes.toBytes("f1"),Bytes.toBytes("h1"));
//        System.out.println("name: "+Bytes.toString(name));

        Scan scan = new Scan(Bytes.toBytes("1"));
        scan.setTimeRange(1570618399294L,1570618799294L);
        ResultScanner rs = table.getScanner(scan);
        scan.setCaching(1000);
        for(Result r : rs){
            String name1 = Bytes.toString(r.getValue(Bytes.toBytes("f1"),Bytes.toBytes("h1")));
        }
    }
}
