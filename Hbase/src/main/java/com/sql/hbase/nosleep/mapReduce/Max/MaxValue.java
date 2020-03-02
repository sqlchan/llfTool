package com.sql.hbase.nosleep.mapReduce.Max;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * setup()，此方法被MapReduce框架仅且执行一次，在执行Map任务前，进行相关变量或者资源的集中初始化工作。若是将资源初始化工作放在方法map()中，导致Mapper任务在解析每一行输入时都会进行资源初始化工作，导致重复，程序运行效率不高！
 * cleanup(),此方法被MapReduce框架仅且执行一次，在执行完毕Map任务后，进行相关变量或资源的释放工作。若是将释放资源工作放入方法map()中，也会导致Mapper任务在解析、处理每一行文本后释放资源，而且在下一行文本解析前还要重复初始化，导致反复重复，程序运行效率不高！
 */
public class MaxValue extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private int maxNum = 0;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] str = value.toString().split(" ");
            try {// 对于非数字字符我们忽略掉
                for(int i=0;i<str.length;i++){
                    int temp = Integer.parseInt(str[i]);
                    if (temp > maxNum) {
                        maxNum = temp;
                    }
                }
            } catch (NumberFormatException e) {
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            context.write(new Text("Max"), new IntWritable(maxNum));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int maxNum = 0;
        private Text one = new Text();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                if ( val.get() > maxNum) {
                    maxNum = val.get();
                }
            }
            one = key;
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            context.write(one, new IntWritable(maxNum));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapred.jar","mv.jar");
        Job job = new Job(conf, "MaxNum");
        job.setJarByClass(MaxValue.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();
        int res = ToolRunner.run(new Configuration(), new MaxValue(), args);
        System.out.println(System.nanoTime()-start);
        System.exit(res);
    }
}

