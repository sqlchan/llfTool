package com.sql.hbase.nosleep.mapReduce.Inverted;

/**
 * 前言："倒排索引"是文档检索系统中最常用的数据结构，被广泛地应用于全文搜索引擎。它主要是用来存储某个单词（或词组）在一个文档或一组文档中的存储位置的映射，即提供了一种根据内容来查找文档的方式。由于不是根据文档来确定文档所包含的内容，而是进行相反的操作，因而称为倒排索引（Inverted Index）
 *
 * (1)这里存在两个问题：第一，<key,value>对只能有两个值，在不使用Hadoop自定义数据类型的情况下，需要根据情况将其中两个值合并成一个值，作为key或value值；第二，通过一个Reduce过程无法同时完成词频统计和生成文档列表，所以必须增加一个Combine过程完成词频统计。
 * (2)这里讲单词和URL组成key值（如"MapReduce：file1.txt"），将词频作为value，这样做的好处是可以利用MapReduce框架自带的Map端排序，将同一文档的相同单词的词频组成列表，传递给Combine过程，实现类似于WordCount的功能。
 * (3)Combine过程：经过map方法处理后，Combine过程将key值相同的value值累加，得到一个单词在文档在文档中的词频，如果直接输出作为Reduce过程的输入，在Shuffle过程时将面临一个问题：所有具有相同单词的记录（由单词、URL和词频组成）应该交由同一个Reducer处理，但当前的key值无法保证这一点，所以必须修改key值和value值。这次将单词作为key值，URL和词频组成value值（如"file1.txt：1"）。这样做的好处是可以利用MapReduce框架默认的HashPartitioner类完成Shuffle过程，将相同单词的所有记录发送给同一个Reducer进行处理。
 *
 *
 * 4.知识点延伸：
 * (1)int indexOf(String str) ：返回第一次出现的指定子字符串在此字符串中的索引。 
 * (2)int indexOf(String str, int startIndex)：从指定的索引处开始，返回第一次出现的指定子字符串在此字符串中的索引。 
 * (3)int lastIndexOf(String str) ：返回在此字符串中最右边出现的指定子字符串的索引。 
 * (4)int lastIndexOf(String str, int startIndex) ：从指定的索引处开始向后搜索，返回在此字符串中最后一次出现的指定子字符串的索引。
 * (5)indexOf("a")是从字符串的0个位置开始查找的。比如你的字符串："abca"，那么程序将会输出0，之后的a是不判断的。
 * (6)str＝str.substring(int beginIndex);截取掉str从首字母起长度为beginIndex的字符串，将剩余字符串赋值给str
 * (7)str＝str.substring(int beginIndex，int endIndex);截取str中从beginIndex开始至endIndex结束时的字符串，并将其赋值给str
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text(); // 存储单词和URL组合
        private Text valueInfo = new Text(); // 存储词频
        private FileSplit split; // 存储Split对象
        // 实现map函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // key值由单词和URL组成，如"MapReduce：file1.txt"
                // 获取文件的完整路径
                // keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
                // 这里为了好看，只获取文件的名称。
                int splitIndex = split.getPath().toString().indexOf("file");
                keyInfo.set(itr.nextToken() + ":" + split.getPath().toString().substring(splitIndex));
                // 词频初始化为1
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
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
            info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
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
                fileList += value.toString() + ";";
            }
            result.set(fileList);
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

