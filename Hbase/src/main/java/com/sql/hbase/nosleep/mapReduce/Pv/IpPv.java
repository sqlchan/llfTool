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

public class IpPv {

    public static class IpPvUvMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text>
                outputCollector, Reporter reporter) throws IOException {
            String ip = text.toString().split(" ", 5)[0];
            outputCollector.collect(new Text("pv"), new Text("1"));
        }
    }

    public static class IpPvUvReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text>
                outputCollector, Reporter reporter) throws IOException {
            long sum = 0;
            while(iterator.hasNext()){
                sum = sum + Long.parseLong(iterator.next().toString());
            }
            outputCollector.collect(new Text("pv"), new Text(String.valueOf(sum)));
        }
    }

    public static void main(String [] args) throws IOException {
        System.out.println(args.length);
        if(args.length < 2){
            System.out.println("args not right!");
            return ;
        }
        JobConf conf = new JobConf(IpPv.class);
        //set output key class
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        //set mapper & reducer class
        conf.setMapperClass(IpPvUvMap.class);
        conf.setCombinerClass(IpPvUvReduce.class);
        conf.setReducerClass(IpPvUvReduce.class);

        // set format
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        String inputDir = args[0];
        String outputDir = args[1];

        // FileInputFormat.setInputPaths(conf, "/user/hadoop/input/");
        FileInputFormat.setInputPaths(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));
        boolean flag = JobClient.runJob(conf).isSuccessful();
    }
}

