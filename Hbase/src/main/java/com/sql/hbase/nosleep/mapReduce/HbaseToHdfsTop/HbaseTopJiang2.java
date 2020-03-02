package com.sql.hbase.nosleep.mapReduce.HbaseToHdfsTop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HbaseTopJiang2{
    public static class doMapper extends TableMapper<Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        	/*不进行分隔，将value整行全部获取
			String rowValue = Bytes.toString(value.list().get(0).getValue());
          	context.write(new Text(rowValue), one);
        	*/
            String[] rowValue = Bytes.toString(value.list().get(0).getValue()).split(" ");
            for (String str: rowValue){
                word.set(str);
                context.write(word,one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int total=0;
            for (IntWritable val : values){
                total++;
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static final int K = 3;
    /**
     * 把上一个mapreduce的结果的key和value颠倒，调到后就可以按照key排序了。
     */
    public static class KMap extends Mapper<LongWritable,Text,IntWritable,Text> {
        TreeMap<Integer, String> map = new TreeMap<Integer, String>();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String result[] = line.split("\t");
            StringBuffer hui = null;
            if(result.length > 2){	//我怕在往hbase表输入数据时带\t分隔符的，后来发现hbase中插入数据的时候根本就不能插入制表符
                for(int i=0;i<result.length-2;i++){
                    hui=new StringBuffer().append(result[i]);
                }
            }else{
                hui = new StringBuffer().append(result[0]);
            }
            if(line.trim().length() > 0 && line.indexOf("\t") != -1) {
                String[] arr = line.split("\t", 2);
                String name = arr[0];
                Integer num = Integer.parseInt(arr[1]);
                if (map.containsKey(num)){
                    String value1 = map.get(num) + "," + hui;
                    map.put(num,value1);
                }
                else {
                    map.put(num, hui.toString());
                }
                if(map.size() > K) {
                    map.remove(map.firstKey());
                }
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            for(Integer num : map.keySet()) {
                context.write(new IntWritable(num), new Text(map.get(num)));
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
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                result.set(val.toString());
                context.write(result, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String tablename = "hello";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "h71");
        Job job1 = new Job(conf, "WordCountHbaseReader");
        job1.setJarByClass(HbaseTopJiang2.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tablename,scan,doMapper.class, Text.class, IntWritable.class, job1);
        job1.setReducerClass(WordCountReducer.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[0]));
        MultipleOutputs.addNamedOutput(job1, "hdfs", TextOutputFormat.class, WritableComparable.class, Writable.class);

        Job job2 = Job.getInstance(conf, "Topjiang");
        job2.setJarByClass(HbaseTopJiang2.class);
        job2.setMapperClass(KMap.class);
        job2.setSortComparatorClass(IntKeyDescComparator.class);
        job2.setPartitionerClass(KeySectionPartitioner.class);
        job2.setReducerClass(SortIntValueReduce.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        //提交job1及job2,并等待完成
        if (job1.waitForCompletion(true)) {
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}

