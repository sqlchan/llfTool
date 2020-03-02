package com.llf.Test;

import com.llf.util.StringEnum;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SunFlink {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource dataSource = env.fromElements(StringEnum.WORDS);
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.toLowerCase().split("\\W+");
                for (String s1 : strings){
                    if (s1.length() > 0 ){
                        collector.collect(new Tuple2<>(s1,1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
        System.out.println(dataSource.count());
    }
}
