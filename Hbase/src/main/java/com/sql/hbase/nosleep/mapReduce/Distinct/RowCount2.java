package com.sql.hbase.nosleep.mapReduce.Distinct;

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

import java.io.IOException;

public class RowCount2 {

    public static class RowCount2Mapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
        public long count = 0;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            count += 1;
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(count), NullWritable.get());
        }
    }

    public static class RowCount2Reducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
        public long count = 0;
        public void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            count += key.get();
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(count), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: FindMax <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "RowCount2");
        job.setJarByClass(RowCount2.class);
        job.setMapperClass(RowCount2Mapper.class);
        job.setCombinerClass(RowCount2Reducer.class);
        job.setReducerClass(RowCount2Reducer.class);
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

