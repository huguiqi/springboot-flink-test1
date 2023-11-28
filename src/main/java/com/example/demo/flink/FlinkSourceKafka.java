package com.example.demo.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @version v1
 * @Author: sam.hu (huguiqi@zaxh.cn)
 * @Copyright (c) 2023, zaxh Group All Rights Reserved.
 * @since: 2023/11/28/18:04
 * @summary:
 */
public class FlinkSourceKafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.199:9991");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"kafka-group-test");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("flinkTopicTest",new SimpleStringSchema(),props);

        DataStream dataStream = env.addSource(consumer);
        dataStream.print();

        env.execute("kafka stream task");

    }
}
