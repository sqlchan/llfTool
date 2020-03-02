package com.sql.hbase.nosleep.mapReduce.other;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MapReduce_Hbase_DownLoad extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MapReduce_Hbase_DownLoad(), args);
    }


    static class  MyMapper extends TableMapper<Text, LongWritable> {
        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws InterruptedException, IOException {
//            for(KeyValue kv:value.raw()){
//                System.out.print(new String(kv.getRow()));
//                System.out.println(new String(kv.getValue()));
//             }
            for (int i = 0; i < value.size(); i++) {
                String val = new String(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
                System.out.println("value :" + val);
            }
        }
    }



    @Override
    public int run(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            Job job = new Job(config,"ExampleReadWrite");
            job.setJarByClass(MapReduce_Hbase_DownLoad.class);     // class that contains mapper
            Scan scan = new Scan();
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);
            TableMapReduceUtil.initTableMapperJob(
                    "student",        // input HBase table name
                    scan,             // Scan instance to control CF and attribute selection
                    MyMapper.class,   // mapper
                    null,             // mapper output key
                    null,             // mapper output value
                    job);
            job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return 0;
    }
}
