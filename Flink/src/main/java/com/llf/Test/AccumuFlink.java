package com.llf.Test;

import com.llf.util.StringEnum;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AccumuFlink {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource dataSource = env.fromElements(StringEnum.WORDS);
        dataSource.flatMap(new RichFlatMapFunction<String,Tuple2<String,Integer>>() {
            private IntCounter intCounter = new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("intCounter",intCounter);
            }

            @Override
            public void flatMap(String s,Collector<Tuple2<String,Integer>> collector) throws Exception {
                String[] strings = s.toLowerCase().split("\\W+");
                for (String s1 : strings){
                    if (s1.length() > 0){
                        collector.collect(new Tuple2<>(s1,1));
                    }
                }
                intCounter.add(1);
            }
        }).groupBy(0).sum(1).print();

        int i = env.getLastJobExecutionResult().getAccumulatorResult("intCounter");
        System.out.println("res: "+ i);
    }
}
