package com.sql.hbase.nosleep.mapReduce.hdfsorhbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

// MapReduce功能实现一---Hbase和Hdfs之间数据相互转换
// 从Hbase表1中读取数据再把统计结果存到表2
// TableMapper   TableMapper<KEYOUT, VALUEOUT>
// IntWritable   一个可比较的整数
// TableReducer<KEYIN, VALUEIN, KEYOUT>
/**
 * Text.class
 * 此类使用标准UTF8编码存储文本。 它提供了在字节级别对文本进行序列化，反序列化和比较的方法。
 * 长度的类型是整数，并使用零压缩格式进行序列化。 另外，它提供了用于遍历字符串的方法，而无需将字节数组转换为字符串。
 * 还包括用于对字符串进行序列化/反序列化，对字符串进行编码/解码，检查字节数组是否包含有效的UTF8代码，计算已编码字符串的长度的实用程序。
 */
public class HBaseToHbase {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String hbaseTableName1 = "hello";
        String hbaseTableName2 = "mytb2";

        prepareTB2(hbaseTableName2);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseToHbase.class);
        job.setJobName("mrreadwritehbase");

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(hbaseTableName1, scan, doMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(hbaseTableName2, doReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }

    public static class doMapper extends TableMapper<Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowValue = Bytes.toString(value.list().get(0).getValue());
            context.write(new Text(rowValue), one);
        }
    }

    public static class doReducer extends TableReducer<Text, IntWritable, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            int sum = 0;
            Iterator<IntWritable> haha = values.iterator();
            while (haha.hasNext()) {
                sum += haha.next().get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("mycolumnfamily"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
            context.write(NullWritable.get(), put);
        }
    }

    public static void prepareTB2(String hbaseTableName) throws IOException{
        HTableDescriptor tableDesc = new HTableDescriptor(hbaseTableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor("mycolumnfamily");
        tableDesc.addFamily(columnDesc);
        Configuration  cfg = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(hbaseTableName)) {
            System.out.println("Table exists,trying drop and create!");
            admin.disableTable(hbaseTableName);
            admin.deleteTable(hbaseTableName);
            admin.createTable(tableDesc);
        } else {
            System.out.println("create table: "+ hbaseTableName);
            admin.createTable(tableDesc);
        }
    }
}

