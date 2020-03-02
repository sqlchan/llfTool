package com.sql.hbase.nosleep.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseCURD {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config)){
            Table table = connection.getTable(TableName.valueOf("llf"));
            Put put = new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("h1"),Bytes.toBytes("11"));
            table.put(put);

            boolean result = table.checkAndPut(Bytes.toBytes("1"),Bytes.toBytes("f1"),Bytes.toBytes("h1"),Bytes.toBytes("11"),put);

            Append append = new Append(Bytes.toBytes("1"));
            append.add(Bytes.toBytes("f1"),Bytes.toBytes("h1"),Bytes.toBytes("11"));
            table.append(append);

            Increment increment = new Increment(Bytes.toBytes("1"));
            increment.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("h1"),10L);
            table.increment(increment);

            Get get = new Get(Bytes.toBytes("1"));
            get.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("h1"));
            Result result1 = table.get(get);
            byte[] name = result1.getValue(Bytes.toBytes("f1"),Bytes.toBytes("h1"));

            Delete delete = new Delete(Bytes.toBytes("1"));
            delete.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("h1"));
            table.delete(delete);

            RowMutations rowMutations = new RowMutations(Bytes.toBytes("1"));
            rowMutations.add(put);
            rowMutations.add(delete);
            table.mutateRow(rowMutations);

            List<Row> action = new ArrayList<>();
            action.add(put);
            action.add(delete);
            Object[] results = new Object[action.size()];
            table.batch(action,results);

            // 没必要，批量put就行
            BufferedMutator bm = connection.getBufferedMutator(TableName.valueOf("llf"));
            bm.mutate(put);
            bm.flush();

            Scan scan = new Scan(Bytes.toBytes("1"));
            ResultScanner rs = table.getScanner(scan);
            scan.setCaching(1000);
            for(Result r : rs){
                String name1 = Bytes.toString(r.getValue(Bytes.toBytes("f1"),Bytes.toBytes("h1")));
            }
            rs.close();

        }
    }

}
