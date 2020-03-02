package com.sql.hbase.nosleep.mapReduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *  HBase MapReduce 读/写 数据迁移 示例 需求场景：【建表后再写】将所有info列簇中的所有name这列导入到另一张表中去
 */
public class TestHBaseMapReduce extends Configured implements Tool {

    // 使用TableMapper读取HBase表的数据 <type of key,  type of value>
    // TableMapper : 扩展基本的Mapper类以添加所需的输入键和值类。
    // ImmutableBytesWritable : 可用作键或值的字节序列。
    // TableMapper<KEYOUT, VALUEOUT> extends Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
    // Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public static class ReadHBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        // 为输入拆分中的每个键/值对调用一次。 大多数应用程序应覆盖此功能，但默认设置是身份功能。
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // 读取表，每行作为一个输入，并取出了rowkey
            Put put = new Put(key.get());
            // 遍历结果集
            for (Cell cell : value.rawCells()) {
                // 判断当前列簇是不是info，再判断列是不是name
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                    if ("age".equals(Bytes.toString(CellUtil
                            .cloneQualifier(cell)))) {
                        // 将info:name列放入put
                        put.add(cell);
                    }
                }
            }
            // map输出
            context.write(key, put);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "hbase-mr");// job名称
        job.setJarByClass(TestHBaseMapReduce.class);
        // job.setJarByClass(ReadHBaseMapper.class);
        Scan scan = new Scan();
        // scan.setCacheBlocks(false); //不需要缓存数据
        // scan.setCaching(500); //每次从服务端读取的行数
        // 设置任务参数
        TableMapReduceUtil.initTableMapperJob("student", // read hbase table name
                scan, // 扫描器
                ReadHBaseMapper.class, // mapper类
                ImmutableBytesWritable.class, // out key class
                Put.class, // out value class
                job);
        TableMapReduceUtil.initTableReducerJob("student2", // out hbase table
                null, // reduce class ---> null
                job);
        // job.setNumReduceTasks(1); //reduce任务个数
        // 提交job
        // 将作业提交到集群，然后等待它完成。
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 读取配置文件
        //Configuration conf = HBaseConfiguration.create();
        // 数据文件存储的路径
        //conf.set("hbase.rootdir", "hdfs://hadoop01.com:8020/hbase");
        // zookeeper连接信息
        //conf.set("hbase.zookeeper.quorum", "hadoop01.com");

        Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum", "10.33.57.46");
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
        //conf.set("fs.default.name","hdfs://SERVICE-HADOOP-admin");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        int status = ToolRunner.run(conf, new TestHBaseMapReduce(), args);
        System.exit(status);
    }
}
