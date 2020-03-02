package com.sql.hbase.nosleep.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HbaseFilterTest {
    private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
    private static final String ZK_CONNECT_VALUE = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    private static Connection conn = null;
    private static Admin admin = null;

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
        Table table = conn.getTable(TableName.valueOf("student"));

        Scan scan = new Scan();

        // 行键过滤器 RowFilter
        Filter rowFilter = new RowFilter(CompareOp.GREATER, new BinaryComparator("95007".getBytes()));
        scan.setFilter(rowFilter);

        // 列簇过滤器 FamilyFilter
        Filter familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator("info".getBytes()));
        scan.setFilter(familyFilter);

        // 列过滤器 QualifierFilter
        Filter qualifierFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
        scan.setFilter(qualifierFilter);

        // 值过滤器 ValueFilter
        Filter valueFilter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("男"));
        scan.setFilter(valueFilter);

        // 时间戳过滤器 TimestampsFilter
        List<Long> list = new ArrayList<>();
        list.add(1522469029503L);
        TimestampsFilter timestampsFilter = new TimestampsFilter(list);
        scan.setFilter(timestampsFilter);

        // 专用过滤器
        // 单列值过滤器 SingleColumnValueFilter —-会返回满足条件的整行
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                       "info".getBytes(),
                       "name".getBytes(),
                       CompareOp.EQUAL,
                       new SubstringComparator("刘晨"));
        singleColumnValueFilter.setFilterIfMissing(true);
        scan.setFilter(singleColumnValueFilter);

        // 单列值排除器 SingleColumnValueExcludeFilter
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(
                       "info".getBytes(),
                       "name".getBytes(),
                       CompareOp.EQUAL,
                       new SubstringComparator("刘晨"));
        singleColumnValueExcludeFilter.setFilterIfMissing(true);
        scan.setFilter(singleColumnValueExcludeFilter);

        // 前缀过滤器 PrefixFilter—-针对行键
        PrefixFilter prefixFilter = new PrefixFilter("9501".getBytes());
        scan.setFilter(prefixFilter);

        // 列前缀过滤器 ColumnPrefixFilter
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter("name".getBytes());
        scan.setFilter(columnPrefixFilter);



        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getRow()) + "\t" + Bytes.toString(cell.getFamily()) + "\t" + Bytes.toString(cell.getQualifier())+ "\t" + Bytes.toString(cell.getValue()) + "\t" + cell.getTimestamp());
            }
        }

    }
}