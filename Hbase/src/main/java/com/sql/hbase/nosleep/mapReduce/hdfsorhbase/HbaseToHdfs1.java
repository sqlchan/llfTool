package com.sql.hbase.nosleep.mapReduce.hdfsorhbase;

// 2.将表1的内容统计输出：
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HbaseToHdfs1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String tablename = "hello";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "h71");
        Job job = new Job(conf, "WordCountHbaseReader");
        job.setJarByClass(HbaseToHdfs1.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tablename,scan,doMapper.class, Text.class, IntWritable.class, job);
        job.setReducerClass(WordCountHbaseReaderReduce.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        MultipleOutputs.addNamedOutput(job, "hdfs", TextOutputFormat.class, WritableComparable.class, Writable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class doMapper extends TableMapper<Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		/*
		String rowValue = Bytes.toString(value.list().get(0).getValue());
	  		context.write(new Text(rowValue), one);
	  	*/
            String[] rowValue = Bytes.toString(value.list().get(0).getValue()).split(" ");
            for (String str: rowValue){
                word.set(str);
                context.write(word,one);
            }
        }
    }

    public static class WordCountHbaseReaderReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int total=0;
            for(IntWritable val:values){
                total++;
            }
            context.write(key, new IntWritable(total));
        }
    }
}

