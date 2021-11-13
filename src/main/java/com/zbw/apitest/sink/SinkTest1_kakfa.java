package com.zbw.apitest.sink;

import com.zbw.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author bevr
 * @create 2021-11-13 22:10
 */
public class SinkTest1_kakfa {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        //从文件中读取
//        DataStream<String> inputStream = env.readTextFile("D:\\idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //从Kafka中读取
        DataStream<String> inputStream = env.addSource( new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

        //转换成sensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0].trim(), new Long(fields[1].trim()), new  Double(fields[2].trim()));
        });

//        dataStream.addSink( new FlinkKafkaProducer011<String>("localhost:9092", "sinktest", new SimpleStringSchema()));

        dataStream.addSink( new FlinkKafkaProducer011<SensorReading>("localhost:9092", "sinktest", new SerializationSchema<SensorReading>() {
            @Override
            public byte[] serialize(SensorReading element) {
                return element.toString().getBytes();
            }
        }));

        env.execute();
    }
}
