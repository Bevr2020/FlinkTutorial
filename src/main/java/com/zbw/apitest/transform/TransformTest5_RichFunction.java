package com.zbw.apitest.transform;

import com.zbw.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bevr
 * @create 2021-11-12 9:45
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //从文件中读取
        DataStream<String> inputStream = env.readTextFile("D:\\idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //转换成sensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0].trim(), new Long(fields[1].trim()), new  Double(fields[2].trim()));
        });

        DataStream<Tuple2<String, Integer>> resultStream =
                dataStream.map( new MyMapper1() );

        resultStream.print();

        env.execute();
    }

    private static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return null;
        }
    }

    //实现自定义富函数类

    public static class MyMapper1 extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作 一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的首尾操作
            System.out.println("close");
        }
    }
}
