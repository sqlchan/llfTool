package com.sql.hbase.nosleep.mapReduce.topN;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top1 {

    public static class TopNMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        int len;
        int top[];
        @Override
        public void setup(Context context) throws IOException,InterruptedException {
            len = context.getConfiguration().getInt("N", 10);
            top = new int[len+1];
        }
        @Override
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
            String line = value.toString();
            String arr []= line.split(",");
            if(arr != null && arr.length == 4){
                int pay = Integer.parseInt(arr[2]);
                add(pay);
            }
        }
        public void add(int pay){
            top[0] = pay;
            Arrays.sort(top);
        }
        @Override
        public void cleanup(Context context) throws IOException,InterruptedException {
            for(int i=1;i<=len;i++){
                context.write(new IntWritable(top[i]),new IntWritable(top[i]));
            }
        }
    }

    public static class TopNReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        int len;
        int top[];
        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            len = context.getConfiguration().getInt("N", 10);
            top = new int[len+1];
        }
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {
            for(IntWritable val : values){
                add(val.get());
            }
        }
        public void add(int pay){
            top[0] = pay;
            Arrays.sort(top);
        }
        @Override
        public void cleanup(Context context)
                throws IOException, InterruptedException {
            for(int i=len;i>0;i--){
                context.write(new IntWritable(len-i+1),new IntWritable(top[i]));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("N", 5);
        Job job = new Job(conf, "my own word count");
        job.setJarByClass(Top.class);
        job.setMapperClass(TopNMapper.class);
        job.setCombinerClass(TopNReduce.class);
        job.setReducerClass(TopNReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(job.waitForCompletion(true));
    }
}

