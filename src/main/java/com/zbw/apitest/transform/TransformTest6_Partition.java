package com.zbw.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bevr
 * @create 2021-11-12 9:45
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件中读取
        DataStream<String> inputStream = env.readTextFile("D:\\idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //1: shuffle

        env.execute();
    }
}
