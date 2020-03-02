package com.sql.hbase.nosleep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DebugTest {
    public static void main(String[] args) throws IOException {
//        Configuration config = HBaseConfiguration.create();
//        try (Connection connection = ConnectionFactory.createConnection(config)) {
//            Table table = connection.getTable(TableName.valueOf("llf"));
//            Put put = new Put(Bytes.toBytes("1"));
//            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("h1"), Bytes.toBytes("11"));
//            table.put(put);
//        }catch (Exception e){
//            e.printStackTrace();
//        }
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.33.57.46");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("fs.default.name","hdfs://SERVICE-HADOOP-admin");
        //System.setProperty("HADOOP_USER_NAME", "hadoop");
        FileSystem fs = FileSystem.get(conf);
        fs.create(new Path("/test"));
    }
}
