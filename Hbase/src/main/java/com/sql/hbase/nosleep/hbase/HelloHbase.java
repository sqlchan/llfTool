package com.sql.hbase.nosleep.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.IOException;

public class HelloHbase {

    public static void createOrOverWriter(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())){
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()){
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("llf"));
            table.addFamily(new HColumnDescriptor("f1").setCompressionType(Compression.Algorithm.NONE));
            System.out.println("create table "+table.getTableName().toString());
            createOrOverWriter(admin,table);
            System.out.println("done.");
        }
    }

    public static void modifySchema (Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()){
            TableName tableName = TableName.valueOf("llf");
            if (!admin.tableExists(tableName)){
                System.out.println("not exist");
                System.exit(-1);
            }
            HColumnDescriptor newColumn = new HColumnDescriptor("f2");
            newColumn.setCompressionType(Compression.Algorithm.SNAPPY);
            newColumn.setMaxVersions(3);
            admin.addColumn(tableName,newColumn);

            HTableDescriptor table = admin.getTableDescriptor(tableName);
            HColumnDescriptor mycf = new HColumnDescriptor("f1");
            mycf.setCompressionType(Compression.Algorithm.SNAPPY);
            table.modifyFamily(mycf);
            admin.modifyTable(tableName,table);
        }
    }

    public static void deleteSchema (Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()){
            TableName tableName = TableName.valueOf("llf");
            admin.disableTable(tableName);
            //admin.deleteColumn(tableName,"f1".getBytes());
            admin.deleteTable(tableName);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        //createSchemaTables(config);
        //modifySchema(config);
        //deleteSchema(config);
    }
}
