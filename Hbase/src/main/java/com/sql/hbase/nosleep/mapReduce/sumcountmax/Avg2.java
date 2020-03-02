package com.sql.hbase.nosleep.mapReduce.sumcountmax;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// 每1个步骤看成一个Job，其中Job3必须等待Job1、Job2完成，并将Job1、Job2的输出结果做为输入，下面的代码演示了如何将这3个Job串起来
public class Avg2 {

    public static boolean deleteFile(Configuration conf, String remoteFilePath, boolean recursive) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        boolean result = fs.delete(new Path(remoteFilePath), recursive);
        fs.close();
        return result;
    }

    public static boolean deleteFile(Configuration conf, String remoteFilePath) throws IOException {
        return deleteFile(conf, remoteFilePath, true);
    }

    private static final Text TEXT_SUM = new Text("SUM");
    private static final Text TEXT_COUNT = new Text("COUNT");
    private static final Text TEXT_AVG = new Text("AVG");

    //计算Sum
    public static class SumMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public long sum = 0;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            sum += Long.parseLong(value.toString());
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_SUM, new LongWritable(sum));
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public long sum = 0;
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            for (LongWritable v : values) {
                sum += v.get();
            }
            context.write(TEXT_SUM, new LongWritable(sum));
        }
    }

    //计算Count
    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public long count = 0;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            count += 1;
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_COUNT, new LongWritable(count));
        }
    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public long count = 0;
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            for (LongWritable v : values) {
                count += v.get();
            }
            context.write(TEXT_COUNT, new LongWritable(count));
        }
    }

    //计算Avg
    public static class AvgMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        public long count = 0;
        public long sum = 0;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] v = value.toString().split("\t");
            if (v[0].equals("COUNT")) {
                count = Long.parseLong(v[1]);
            } else if (v[0].equals("SUM")) {
                sum = Long.parseLong(v[1]);
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(sum), new LongWritable(count));
        }
    }


    public static class AvgReducer extends Reducer<LongWritable, LongWritable, Text, DoubleWritable> {
        public long sum = 0;
        public long count = 0;
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            sum += key.get();
            for (LongWritable v : values) {
                count += v.get();
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_AVG, new DoubleWritable(new Double(sum) / count));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = "/input/ceshi.txt";
        String maxOutputPath = "/output/sum/";
        String countOutputPath = "/output/count/";
        String avgOutputPath = "/output/avg/";

        //删除输出目录(可选,省得多次运行时,总是报OUTPUT目录已存在)
        Avg2.deleteFile(conf, maxOutputPath);
        Avg2.deleteFile(conf, countOutputPath);
        Avg2.deleteFile(conf, avgOutputPath);

        Job job1 = Job.getInstance(conf, "Sum");
        job1.setJarByClass(Avg2.class);
        job1.setMapperClass(SumMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(maxOutputPath));


        Job job2 = Job.getInstance(conf, "Count");
        job2.setJarByClass(Avg2.class);
        job2.setMapperClass(CountMapper.class);
        job2.setCombinerClass(CountReducer.class);
        job2.setReducerClass(CountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(countOutputPath));


        Job job3 = Job.getInstance(conf, "Average");
        job3.setJarByClass(Avg2.class);
        job3.setMapperClass(AvgMapper.class);
        job3.setReducerClass(AvgReducer.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        //将job1及job2的输出为做job3的输入
        FileInputFormat.addInputPath(job3, new Path(maxOutputPath));
        FileInputFormat.addInputPath(job3, new Path(countOutputPath));
        FileOutputFormat.setOutputPath(job3, new Path(avgOutputPath));

        //提交job1及job2,并等待完成
        if (job1.waitForCompletion(true) && job2.waitForCompletion(true)) {
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }
    }
}

