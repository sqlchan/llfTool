package com.sql.hbase.nosleep.mapReduce.topN;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 问题：在程序中有这么一行代码context.getConfiguration().getInt("N", 10);实在是没理解10这个参数有什么用，而且我后来测试将10改为其他值也不影响结果啊。"N"还不能瞎改，map和reduce函数的这个字符还必须和主函数中的一样，我后来将主函数的这个字符改为和其他地方不一样后，输出的结果不以主函数的数字为主，而是以其他地方的数字为主
 * 答疑：setInt(String name, int value)为在本地properties中加入(name,value)。getInt(String name, int defaultValue)为尝试查询name对应的整数，失败则返回defaultValue
 */
public class TopN2 {

    public static class MyMapper extends Mapper<Object, Text, NullWritable, IntWritable>{
        private TreeMap<Integer, Integer> tree = new TreeMap<Integer, Integer>();
        //      private final static IntWritable one = new IntWritable(1);
//      private Text number = new Text();
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
//          super.setup(context);
            System.out.println("Mapper("+context.getConfiguration().getInt("N", 10)+"):in setup...");
        }
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
//            super.cleanup(context);
            System.out.println("Mapper("+context.getConfiguration().getInt("N", 10)+"):in cleanup...");
            for(Integer text : tree.values()){
                context.write(NullWritable.get(), new IntWritable(text));
            }
        }
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String key_num = value.toString();
            int num = Integer.parseInt(key_num);
            tree.put(num, num);
            if(tree.size() > context.getConfiguration().getInt("N", 10))
                tree.remove(tree.firstKey());
//            System.out.println("Mapper("+context.getConfiguration().getInt("N", 10)+"):"+key.toString()+"/"+value.toString());
//            number.set(key_num);
//            context.write(number, one);
        }
    }

    public static class MyReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable>{
        //        private IntWritable kk = new IntWritable();
        private TreeMap<Integer, Integer> tree = new TreeMap<Integer, Integer>();
        //        private IntWritable result = new IntWritable();
        @Override
        public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            for (IntWritable value : values){
                tree.put(value.get(), value.get());
                if(tree.size() > context.getConfiguration().getInt("N", 10))
                {
                    tree.remove(tree.firstKey());
                }
            }
//            System.out.println("Reducer("+context.getConfiguration().getInt("N", 10)+"):"+key.toString()+"/"+result.get());
        }
        @Override
        protected void cleanup(
                org.apache.hadoop.mapreduce.Reducer.Context context)
                throws IOException, InterruptedException {
//            super.cleanup(context);
            for(Integer val : tree.descendingKeySet()){
                context.write(NullWritable.get(), new IntWritable(val));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 3){
            System.err.println("heheda");
            System.exit(2);
        }
        conf.setInt("N", new Integer(otherArgs[0]));
        System.out.println("N:"+otherArgs[0]);

        Job job = Job.getInstance(conf, "TopN");
        job.setJarByClass(TopN2.class);
        job.setMapperClass(MyMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 1; i < otherArgs.length-1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
