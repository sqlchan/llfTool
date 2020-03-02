package com.sql.hbase.nosleep.mapReduce.Max;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sum {

    public static class SumMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
        public long sum = 0;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            sum += Long.parseLong(value.toString());
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(sum), NullWritable.get());
        }
    }

    public static class SumReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
        public long sum = 0;
        public void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            sum += key.get();
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(sum), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Sum <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Sum");
        job.setJarByClass(Sum.class);
        job.setMapperClass(SumMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

