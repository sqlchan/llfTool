package com.sql.hbase.nosleep.mapReduce.HbaseToHdfsTop;

import java.io.IOException;
import java.util.Comparator;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HbaseTopJiang1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tablename = "hello";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "h71");
        Job job = new Job(conf, "WordCountHbaseReader");
        job.setJarByClass(HbaseTopJiang1.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tablename,scan,doMapper.class, Text.class, IntWritable.class, job);
        job.setReducerClass(WordCountHbaseReaderReduce.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        MultipleOutputs.addNamedOutput(job, "hdfs", TextOutputFormat.class, WritableComparable.class, Writable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class doMapper extends TableMapper<Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                           Context context) throws IOException, InterruptedException {
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

    public static final int K = 3;
    public static class WordCountHbaseReaderReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
            }else {
                treeMap.put(sum, key.toString());
            }
            if(treeMap.size() > K) {
                treeMap.remove(treeMap.lastKey());
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //将treeMap中的结果,按value-key顺序写入contex中
            for (Integer key : treeMap.keySet()) {
                context.write(new Text(treeMap.get(key)), new IntWritable(key));
            }
        }
    }
}

