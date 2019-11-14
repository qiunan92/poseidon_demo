package com.qiunan.data_sync.flinkdemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.qiunan.data_sync.rmqflink.RocketMQConfig;
import com.qiunan.data_sync.rmqflink.RocketMQSource;
import com.qiunan.data_sync.rmqflink.common.serialization.SimpleKeyValueDeserializationSchema;
import com.qiunan.data_sync.common.Constant;
import com.qiunan.data_sync.common.DBConnect;
import com.qiunan.data_sync.dws.entity.BinLogMsgEntity;

import java.util.Map;
import java.util.Properties;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * FLINK的window算子的demo
 * 
 * @company GeekPlus
 * @project jaguar
 * @author qiunan
 * @date Apr 30, 2019
 * @since 1.0.0
 */
public class WindowDemo {
    private static Logger logger = Logger.getLogger(WindowDemo.class);

    public static void main(String[] args) {
        try {
            // 1.初始化两个数据源
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Properties consumerProps = new Properties();
            consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, Constant.SOURCE_NAME_SERVER_ADDR);
            consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "flink_demo");
            consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "qiunan_test");
            
            // 2.初始化数据源,
            DataStream<String> dataStream1 = env
                .addSource(new RocketMQSource(
                    new SimpleKeyValueDeserializationSchema(Constant.MQ_CONSTANT_ID, Constant.MQ_CONSTANT_ADDRESS),
                    consumerProps))
                .name("source1").setParallelism(1)
//                .filter(new FilterFunction<Map<String,String>>() {
//
//                    @Override
//                    public boolean filter(Map<String,String> value) throws Exception {
//                            return false;
//                    }
//                })
                .map(new MapFunction<Map, BinLogMsgEntity>() {

                    @Override
                    public BinLogMsgEntity map(Map value) throws Exception {
                        BinLogMsgEntity msgEntity =
                            JSON.parseObject((String)value.get(Constant.MQ_CONSTANT_ADDRESS), new TypeReference<BinLogMsgEntity>() {});
                        return msgEntity;
                    }

                })
                .keyBy(new KeySelector<BinLogMsgEntity, String>() {
                    @Override
                    public String getKey(BinLogMsgEntity value) throws Exception {
                        return value.getTable();
                    }
                }).timeWindow(Time.seconds(5),Time.seconds(2))
                .apply(new WindowFunction<BinLogMsgEntity, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<BinLogMsgEntity> input, Collector<String> out)
                        throws Exception {
                        System.out.println("------------------------------------------------");
                        String sqlStr =
                          "select sum(plan_amount) as plan_amount,unix_timestamp() as time_sec from warehouse.dwd_wms_pick_order where generate_time >= (unix_timestamp()-3600)*1000 ";
                      PreparedStatement preparedStatement = DBConnect.getSourceConnection().prepareStatement(sqlStr);
                      ResultSet resultSet = preparedStatement.executeQuery();
                      while (resultSet.next()) {
                          Integer amount = resultSet.getInt("plan_amount");
                          Long time = resultSet.getLong("time_sec");
                          StringBuffer sb = new  StringBuffer("time:");
                          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                          String timeStr = sdf.format(new Date(time*1000));
                          sb.append(timeStr).append(",amount:").append(amount);
                          out.collect(sb.toString()); 
                      }
                    }
                    });
//                .timeWindowAll(Time.seconds(2))
//                .apply(new AllWindowFunction<Map<String,String>, String, Window>() {
//
//                    @Override
//                    public void apply(Window window, Iterable<Map<String, String>> values, Collector<String> out)
//                        throws Exception {
//                        String sqlStr =
//                            "select sum(plan_amount) as plan_amount,unix_timestamp() as time_sec from warehouse.dwd_wms_pick_order where generate_time >= (unix_timestamp()-3600)*1000 ";
//                        PreparedStatement preparedStatement = DBConnect.getSourceConnection().prepareStatement(sqlStr);
//                        ResultSet resultSet = preparedStatement.executeQuery();
//                        while (resultSet.next()) {
//                            Integer amount = resultSet.getInt("plan_amount");
//                            Long time = resultSet.getLong("time_sec");
//                            StringBuffer sb = new  StringBuffer("time:");
//                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                            String timeStr = sdf.format(new Date(time*1000));
//                            sb.append(timeStr).append(",amount:").append(amount);
//                            out.collect(sb.toString()); 
//                        }
//                        
//                    }});
            dataStream1.print();
            
            
      
            
            //3.执行数据流
            env.execute("geekplus_dws_etl_job1");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("error:" + e.getMessage());
        }
    }

}
