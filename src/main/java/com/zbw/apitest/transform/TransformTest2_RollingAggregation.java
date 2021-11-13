package com.zbw.apitest.transform;

import com.zbw.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bevr
 * @create 2021-11-12 9:45
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件中读取
        DataStream<String> inputStream = env.readTextFile("D:\\idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //转换成sensorReading
        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0].trim(), new Long(fields[1].trim()), new  Double(fields[2].trim()));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy( SensorReading::getId );

        // 滚动聚合，取当前最大的温度值
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print();

        env.execute();
    }
}
