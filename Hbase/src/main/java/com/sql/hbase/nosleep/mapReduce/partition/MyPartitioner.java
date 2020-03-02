package com.sql.hbase.nosleep.mapReduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyPartitioner {

    public static class MyPartitionerMap extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String arr_value[] = value.toString().split("\t");
            if (arr_value.length > 3) {
                context.write(new Text("long"), value);
            } else if (arr_value.length < 3) {
                context.write(new Text("short"), value);
            } else {
                context.write(new Text("right"), value);
            }
        }
    }

    /**
     * partitioner的输入就是map的输出
     */
    public static class MyPartitionerPar extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            int result = 0;
            /*********************************************************************/
            /***key.toString().equals("long")  must use toString()!!!!  ***/
            /***开始的时候我没有用 ，导致都在一个区里，结果也在一个reduce输出文件中。  ***/
            /********************************************************************/
            if (key.toString().equals("long")) {
                result = 0 % numPartitions;
            } else if (key.toString().equals("short")) {
                result = 1 % numPartitions;
            } else if (key.toString().equals("right")) {
                result = 2 % numPartitions;
            }
            return result;
        }
    }

    public static class MyPartitionerReduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, java.lang.Iterable<Text> value, Context context) throws java.io.IOException,
                InterruptedException {
            for (Text val : value) {
                context.write(key, val);
                //context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MyPartitioner <in> <out>");
            System.exit(2);
        }
        conf.set("mapred.jar","mp1.jar");
        Job job = new Job(conf, "MyPartitioner");
        job.setNumReduceTasks(3);

        job.setJarByClass(MyPartitioner.class);

        job.setMapperClass(MyPartitionerMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(MyPartitionerPar.class);
        job.setReducerClass(MyPartitionerReduce.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

