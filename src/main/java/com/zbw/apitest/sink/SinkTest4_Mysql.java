package com.zbw.apitest.sink;

import com.zbw.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author bevr
 * @create 2021-11-13 22:10
 */
public class SinkTest4_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件中读取
        DataStream<String> inputStream = env.readTextFile("D:\\idea\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //转换成sensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0].trim(), new Long(fields[1].trim()), new  Double(fields[2].trim()));
        });

        dataStream.addSink( new MysqlSinkFunction() );

        env.execute();
    }

    private static class MysqlSinkFunction extends RichSinkFunction<SensorReading> {
        Connection conn = null;
        PreparedStatement insertPs = null;
        PreparedStatement updatePs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root", "123456");
            insertPs = conn.prepareStatement("insert into sensor(id, temp) values (?,?)");
            updatePs = conn.prepareStatement("update sensor set temp = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            insertPs.close();
            updatePs.close();
            if(conn != null && !conn.isClosed()){
                conn.close();
            }
        }

        //每来一条语句执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //直接执行更新语句，如果没有更新那么就插入
            updatePs.setDouble(1, value.getTemperature());
            updatePs.setString(2, value.getId());
            boolean execute = updatePs.execute();
            if(execute) {
                insertPs.setString(1, value.getId());
                insertPs.setDouble(2, value.getTemperature());
                insertPs.execute();
            }
        }
    }
}
