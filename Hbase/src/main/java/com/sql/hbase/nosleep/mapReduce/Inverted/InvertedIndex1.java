package com.sql.hbase.nosleep.mapReduce.Inverted;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// 题目：将数据源文件上传到HDFS系统进行存储，然后基于MapReduce编程进行数据分析，测试数据来源于美国专利文献数据
public class InvertedIndex1 {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private final static Text one = new Text("1");
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line[] = value.toString().split(",");
            word.set(line[4].substring(1, line[4].length()-1)+":"+line[1]);
            context.write(word, one);
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();
        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 统计词频
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            int splitIndex = key.toString().indexOf(":");
            // 重新设置value值由URL和词频组成
            info.set("<"+key.toString().substring(splitIndex + 1) + ":" + sum+">");
            // 重新设置key值为单词
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 生成文档列表
            String fileList = new String();
            for (Text value : values) {
                fileList += value.toString() + ",";
            }
            result.set(fileList.substring(0,fileList.length()-1));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.jar", "ii.jar");

        String[] ioArgs = new String[] { "index_in", "index_out" };
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Inverted Index <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

