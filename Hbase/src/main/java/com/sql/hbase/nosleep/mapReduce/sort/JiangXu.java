package com.sql.hbase.nosleep.mapReduce.sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JiangXu {

    public static class SortIntValueMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable wordCount = new IntWritable(1);
        private Text word = new Text();
        public SortIntValueMapper() {
            super();
        }
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().trim());
                wordCount.set(Integer.valueOf(tokenizer.nextToken().trim()));
                context.write(wordCount, word);
            }
        }
    }

    /**
     * 按照key的大小来划分区间，当然，key是int值
     */
    public static class KeySectionPartitioner<K, V> extends Partitioner<K, V> {
        @Override
        public int getPartition(K key, V value, int numReduceTasks) {
            /**
             * int值的hashcode还是自己本身的数值
             */
            //这里我认为大于maxValue的就应该在第一个分区
            int maxValue = 50;
            int keySection = 0;
            // 只有传过来的key值大于maxValue 并且numReduceTasks比如大于1个才需要分区，否则直接返回0
            if (numReduceTasks > 1 && key.hashCode() < maxValue) {
                int sectionValue = maxValue / (numReduceTasks - 1);
                int count = 0;
                while ((key.hashCode() - sectionValue * count) > sectionValue) {
                    count++;
                }
                keySection = numReduceTasks - 1 - count;
            }
            return keySection;
        }
    }

    /**
     * int的key按照降序排列
     */
    public static class IntKeyDescComparator extends WritableComparator {
        protected IntKeyDescComparator() {
            super(IntWritable.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    /**
     * 把key和value颠倒过来输出
     */
    public static class SortIntValueReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
        private Text result = new Text();
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                result.set(val.toString());
                context.write(result, key);
            }
        }
    }

    public static void main(String [] args) throws Exception {
        /**
         * 这里是map输出的key和value类型
         */
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(JiangXu.class);
        job.setMapperClass(SortIntValueMapper.class);
        job.setSortComparatorClass(IntKeyDescComparator.class);
        job.setPartitionerClass(KeySectionPartitioner.class);
        job.setReducerClass(SortIntValueReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        /**
         *这里可以放输入目录数组，也就是可以把上一个job所有的结果都放进去
         **/
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

