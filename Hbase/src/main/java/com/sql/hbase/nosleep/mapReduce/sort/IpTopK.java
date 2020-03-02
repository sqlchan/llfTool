package com.sql.hbase.nosleep.mapReduce.sort;

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

// 将IP统计并升序输出
public class IpTopK {

    public static class IpTopKMapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text>
                outputCollector, Reporter reporter) throws IOException {
            String ip = text.toString().split(" ", 5)[0];
            outputCollector.collect(new Text(ip), new Text("1"));
        }
    }

    public static class IpTopKReducer1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text>
                outputCollector, Reporter reporter) throws IOException {
            long sum = 0;
            while(iterator.hasNext()){
                sum = sum + Long.parseLong(iterator.next().toString());
            }
            outputCollector.collect(new Text(key), new Text(String.valueOf(sum)));
            /**
             * ip1 count
             * ip2 count
             * ip3 count
             */
        }
    }


    public static class IpTopKMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<LongWritable, Text>
                outputCollector, Reporter reporter) throws IOException {
            String [] ks = text.toString().split("\t");
            /**
             * ks[0] , ip
             * ks[1], count
             */
            outputCollector.collect(new LongWritable(Long.parseLong(ks[1])), new Text(ks[0]));
        }
    }

    public static class IpTopKReducer2 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterator<Text> iterator, OutputCollector<LongWritable, Text>
                outputCollector, Reporter reporter) throws IOException {

            while(iterator.hasNext()){
                outputCollector.collect(key, iterator.next());
            }
        }
    }

    public static void main(String [] args) throws IOException {
        System.out.println(args.length);
        if(args.length < 2){
            System.out.println("args not right!");
            return ;
        }

        JobConf conf = new JobConf(IpTopK.class);
        conf.set("mapred.jar","tt.jar");
        //set output key class
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //set mapper & reducer class
        conf.setMapperClass(IpTopKMapper1.class);
        conf.setCombinerClass(IpTopKReducer1.class);
        conf.setReducerClass(IpTopKReducer1.class);

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
            System.out.println("run job-1 successful");
            JobConf conf1 = new JobConf(IpTopK.class);
            conf1.set("mapred.jar","tt.jar");
            //set output key class
            conf1.setOutputKeyClass(LongWritable.class);
            conf1.setOutputValueClass(Text.class);
            //set mapper & reducer class
            conf1.setMapperClass(IpTopKMapper2.class);
            conf1.setReducerClass(IpTopKReducer2.class);
            // set format
            conf1.setInputFormat(TextInputFormat.class);
            conf1.setOutputFormat(TextOutputFormat.class);
            conf1.setNumReduceTasks(1);

            // FileInputFormat.setInputPaths(conf, "/user/hadoop/rongxin/locationinput/");
            FileInputFormat.setInputPaths(conf1, outputDir);
            FileOutputFormat.setOutputPath(conf1, new Path(outputDir + "-2"));
            boolean flag1 = JobClient.runJob(conf1).isSuccessful();
            if(flag1){
                System.out.println("run job-2 successful !!");
            }
        }
    }
}

