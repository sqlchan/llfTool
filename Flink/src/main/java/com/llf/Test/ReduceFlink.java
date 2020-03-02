package com.llf.Test;

import com.llf.util.StringEnum;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ReduceFlink {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(StringEnum.WORDS).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.toLowerCase().split("\\W+");
                for (String s1 : strings){
                    if (s1.length() > 0 ){
                        collector.collect(new Tuple2<>(s1,1));
                    }
                }
            }
        }).groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f0,stringIntegerTuple2.f1+t1.f1);
            }
        }).print();

    }
}
