package com.sql.hbase.nosleep.mapReduce.hdfsorhbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

// 读取Hdfs文件将统计结果存入到Hbase表中
public class HdfsToHBase {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable i = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s[] = value.toString().trim().split("/n");
            for (String m : s) {
                context.write(new Text(m), i);
            }
        }
    }

    public static class Reduce extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            // 列族为cf，列为count，列值为数目
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
            context.write(NullWritable.get(), put);
        }
    }

    public static void createHBaseTable(String tableName) throws IOException {
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor col = new HColumnDescriptor("cf");
        htd.addFamily(col);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "h71");
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table exists, trying to recreate table......");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        System.out.println("create new table:" + tableName);
        admin.createTable(htd);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //将结果存入hbase的表名
        String tableName = "mytb2";
        Configuration conf = new Configuration();
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        createHBaseTable(tableName);
        String input = args[0];
        Job job = new Job(conf, "WordCount table with " + input);
        job.setJarByClass(HdfsToHBase.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
//	FileInputFormat.setInputPaths(job, new Path(input));	//这种方法也可以
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

