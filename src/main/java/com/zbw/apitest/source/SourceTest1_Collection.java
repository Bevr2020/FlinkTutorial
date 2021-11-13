package com.zbw.apitest.source;

import com.zbw.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author bevr
 * @create 2021-11-12 9:04
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从集合中读取
        List<SensorReading> dataList = Arrays.asList(
                new SensorReading("sensor_1", 154718201L, 12.3),
                new SensorReading("sensor_2", 154718223L, 15.3),
                new SensorReading("sensor_3", 154718267L, 17.3)
                );

        DataStream<SensorReading> dataStream = env.fromCollection(dataList);

        DataStream<Integer> intDataStream = env.fromElements(1, 2, 3, 4, 567);

        dataStream.print();
        intDataStream.print();

        env.execute();

    }
}
