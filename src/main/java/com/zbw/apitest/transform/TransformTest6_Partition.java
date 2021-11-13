package com.zbw.apitest.transform;

import com.zbw.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
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

        //1: shuffle 随机打散
//        DataStream<String> shuffleStream = inputStream.shuffle();

//        shuffleStream.print("shuffle");

        //2: keyBy 按照指定key的hash结果选择分区号
        //转换成sensorReading
//        DataStream<SensorReading> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0].trim(), new Long(fields[1].trim()), new  Double(fields[2].trim()));
//        });
//
//        KeyedStream<SensorReading, Tuple> keyByStream = dataStream.keyBy("id");
//        keyByStream.print();

        //3： global 全部数据发送到下一个同一个分区
        inputStream.global().print("global");

        env.execute();
    }
}
