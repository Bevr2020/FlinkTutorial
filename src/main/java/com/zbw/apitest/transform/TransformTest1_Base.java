package com.zbw.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author bevr
 * @create 2021-11-12 9:45
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件中读取
        DataStream<String> inputStream = env.readTextFile("D:\\idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 1 map :把string转换成长度输出
//        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
//            public Integer map(String value) throws Exception {
//                return value.length();
//            }
//        });

        //2 flatMap 按逗号分字段
//        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out) throws Exception {
//                String[] splitStr = value.split(",");
//                for (String str : splitStr){
//                    out.collect(str);
//                }
//            }
//        });

        //3 filter 赛选sensor_1开头的数据
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        filterStream.print();

        env.execute();
    }
}
