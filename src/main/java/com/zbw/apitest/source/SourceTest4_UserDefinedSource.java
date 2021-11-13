package com.zbw.apitest.source;

import com.zbw.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * @author bevr
 * @create 2021-11-12 9:04
 */
public class SourceTest4_UserDefinedSource {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件中读取
        DataStream<SensorReading> dataStream = env.addSource( new MySensorSource() );

        dataStream.print();

        env.execute();

    }

    private static class MySensorSource implements org.apache.flink.streaming.api.functions.source.SourceFunction<SensorReading> {
        //定义一个标志位，用来控制数据的产生
        private boolean running = true;

        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //定义一个随机数发生器
            Random random = new Random();
            Map<String, Double> sensorTempMap = new HashMap<String, Double>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian()*20);
            }

            while (running) {
                Iterator<Map.Entry<String, Double>> itr = sensorTempMap.entrySet().iterator();
                while(itr.hasNext()){
                    Map.Entry<String, Double> next = itr.next();
                    String id = next.getKey();
                    Double tempValue = next.getValue() + random.nextGaussian();
                    ctx.collect(new SensorReading(id, System.currentTimeMillis(), tempValue));
                }
                Thread.sleep(1000L);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
