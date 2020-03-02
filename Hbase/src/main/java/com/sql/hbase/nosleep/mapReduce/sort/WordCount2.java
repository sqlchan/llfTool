package com.sql.hbase.nosleep.mapReduce.sort;

import java.io.IOException;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


// 情况3：改进型的WordCount(按词频倒排)，官网示例WordCount只统计出单词出现的次数,并未按词频做倒排
//来自：http://www.cnblogs.com/yjmyzz/p/hadoop-mapreduce-2-sample.html
//原理: 依然用到了cleanup,此外为了实现排序,采用了TreeMap这种内置了key排序的数据结构.
public class WordCount2 {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //定义treeMap来保持统计结果,由于treeMap是按key升序排列的,这里要人为指定Comparator以实现倒排
        private TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                return y.compareTo(x);
            }
        });
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //reduce后的结果放入treeMap,而不是向context中记入结果
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (treeMap.containsKey(sum)){
                String value = treeMap.get(sum) + "," + key.toString();
                treeMap.put(sum,value);
            }
            else {
                treeMap.put(sum, key.toString());
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //将treeMap中的结果,按value-key顺序写入contex中
            for (Integer key : treeMap.keySet()) {
                context.write(new Text(treeMap.get(key)), new IntWritable(key));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount2 <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count2");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

