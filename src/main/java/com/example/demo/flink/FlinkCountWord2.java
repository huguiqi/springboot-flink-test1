package com.example.demo.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
public class FlinkCountWord2 {


    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.读取文件
        DataStream<String> lineDS = env.readTextFile(ClassLoader.getSystemClassLoader().getResource("input/word.txt").getPath());
        // 2.转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordsAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });

        // 3.按照word进行分组
//        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGS = wordsAndOne
        KeyedStream<Tuple2<String, Integer>, Tuple>  keyedStream =  wordsAndOne.keyBy(0);
        // 4.分组内聚合统计
//        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGS.sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        // 5.打印结果
        sum.print();

        env.execute("字数统计-hgq");

    }
}
