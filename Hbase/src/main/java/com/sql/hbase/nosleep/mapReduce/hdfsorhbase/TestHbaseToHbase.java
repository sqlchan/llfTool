package com.sql.hbase.nosleep.mapReduce.hdfsorhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
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
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Iterator;

public class TestHbaseToHbase {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String table1 ="llf";
        String table2 = "mac";
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(TestHbaseToHbase.class);
        job.setJobName("job");

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(table1,scan,doMapper.class,Text.class,IntWritable.class,job);
        TableMapReduceUtil.initTableReducerJob(table2,doReduce.class,job);
        System.exit(job.waitForCompletion(true) ? 1: 0);
    }
    public static class doMapper extends TableMapper<Text,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowvalue = Bytes.toString(value.list().get(0).getValue());
            context.write(new Text(rowvalue),one);
        }
    }
    public static class doReduce extends TableReducer<Text,IntWritable,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            Iterator<IntWritable> iterable = values.iterator();
            while (iterable.hasNext()){
                sum += iterable.next().get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("in"),Bytes.toBytes("1"),Bytes.toBytes(String.valueOf(sum)));
            context.write(NullWritable.get(),put);
        }
    }

}
