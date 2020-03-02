package com.sql.hbase.nosleep.mapReduce.Pv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 在reduce端直接计算Uv
 */
public class IpUv {

    public static  class IpUvMapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable longWritable, Text text,
                        OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String ip = text.toString().split(" ", 5)[0];
            outputCollector.collect(new Text(ip.trim()), new Text("1"));
        }
    }

    public static class IpUvReducer1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text text, Iterator<Text> iterator,
                           OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            outputCollector.collect(text, new Text("1"));
        }
    }

    public static class IpUvMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable longWritable, Text text,
                        OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String ip = text.toString().split("\t")[0];
            outputCollector.collect(new Text("uv"), new Text("1"));
        }
    }

    public static class IpUvReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text text, Iterator<Text> iterator,
                           OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            long sum = 0;
            /**
             * uv, [1,1,1,1,1,1]
             */
            while(iterator.hasNext()){
                sum = sum + Long.parseLong(iterator.next().toString());
            }
            outputCollector.collect(new Text("uv"), new Text(String.valueOf(sum)));
        }
    }

    public static void main(String [] args) throws IOException {
        if(args.length < 2){
            System.out.println("args not right!");
            return ;
        }
        JobConf conf = new JobConf(IpUv.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //set mapper & reducer class
        conf.setMapperClass(IpUvMapper1.class);
        conf.setReducerClass(IpUvReducer1.class);

        // set format
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        String inputDir = args[0];
        String outputDir = args[1];

        // FileInputFormat.setInputPaths(conf, "/user/hadoop/rongxin/locationinput/");
        FileInputFormat.setInputPaths(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));
        boolean flag = JobClient.runJob(conf).isSuccessful();

        if(flag){
            JobConf conf1 = new JobConf(IpUv.class);
            conf1.setOutputKeyClass(Text.class);
            conf1.setOutputValueClass(Text.class);

            //set mapper & reducer class
            conf1.setMapperClass(IpUvMapper2.class);
            conf1.setReducerClass(IpUvReducer2.class);

            // set format
            conf1.setInputFormat(TextInputFormat.class);
            conf1.setOutputFormat(TextOutputFormat.class);

            // FileInputFormat.setInputPaths(conf, "/user/hadoop/rongxin/locationinput/");
            FileInputFormat.setInputPaths(conf1, outputDir);
            FileOutputFormat.setOutputPath(conf1, new Path(outputDir + "-2"));
            boolean flag1 = JobClient.runJob(conf1).isSuccessful();
            System.out.println(flag1);
        }
    }
}

