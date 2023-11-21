package com.example.demo.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 *
 * @version v1
 * @Author: sam.hu (huguiqi@zaxh.cn)
 * @Copyright (c) 2023, zaxh Group All Rights Reserved.
 * @since: 2023/11/21/16:07
 * @summary:
 */
public class FlinkCountWord {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env
//                = StreamExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("192.168.0.38",6123);
        // 1.读取文件
        DataSource<String> lineDS = env.readTextFile(ClassLoader.getSystemClassLoader().getResource("input/word.txt").getPath());
        // 2.转换数据格式
        FlatMapOperator<String, Tuple2<String, Integer>> wordsAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });

        // 3.按照word进行分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGS = wordsAndOne.groupBy(0);

        // 4.分组内聚合统计
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGS.sum(1);

        // 5.打印结果
        sum.print();

    }
}
