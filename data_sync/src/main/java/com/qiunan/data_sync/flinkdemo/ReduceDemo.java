package com.qiunan.data_sync.flinkdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.qiunan.data_sync.rmqflink.RocketMQConfig;
import com.qiunan.data_sync.rmqflink.RocketMQSource;
import com.qiunan.data_sync.rmqflink.common.serialization.SimpleKeyValueDeserializationSchema;
import com.qiunan.data_sync.common.Constant;
import com.qiunan.data_sync.dws.entity.BinLogMsgEntity;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;

/**
 * FLINK的reduce算子的demo
 * 
 * 
 * @company GeekPlus
 * @project jaguar
 * @author qiunan
 * @date Apr 30, 2019
 * @since 1.0.0
 */
public class ReduceDemo {
    private static Logger logger = Logger.getLogger(ReduceDemo.class);

    public static void main(String[] args) {
        try {
            // 1.初始化数据源
            StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();

            Properties consumerProps1 = new Properties();
            consumerProps1.setProperty(RocketMQConfig.NAME_SERVER_ADDR, Constant.SOURCE_NAME_SERVER_ADDR);
            consumerProps1.setProperty(RocketMQConfig.CONSUMER_GROUP, "flink_demo");
            consumerProps1.setProperty(RocketMQConfig.CONSUMER_TOPIC, "BinLogFromCanal");

            // 2.初始化数据源,对数据源进行映射，过滤，根据表名分成多个侧数据流
            DataStream<BinLogMsgEntity> dataStream1 = env1
                .addSource(new RocketMQSource(
                    new SimpleKeyValueDeserializationSchema(Constant.MQ_CONSTANT_ID, Constant.MQ_CONSTANT_ADDRESS),
                    consumerProps1))
                .name("source1").setParallelism(1)
                .map(new MapFunction<Map<String, String>, BinLogMsgEntity>() {
                    @Override
                    public BinLogMsgEntity map(Map<String, String> value) throws Exception {
                        BinLogMsgEntity msgEntity = JSON.parseObject(value.get(Constant.MQ_CONSTANT_ADDRESS),
                            new TypeReference<BinLogMsgEntity>() {});
                        return msgEntity;
                    }
                })
                .filter(new FilterFunction<BinLogMsgEntity>() {

                    @Override
                    public boolean filter(BinLogMsgEntity value) throws Exception {
                        if(null == value.getData() ) {
                            return false;
                        }else {
                            return true;
                        }
                    }})
                .keyBy("table").timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<BinLogMsgEntity>() {

                    @Override
                    public BinLogMsgEntity reduce(BinLogMsgEntity value1, BinLogMsgEntity value2) throws Exception {
                        System.out.println("-------------------redeceFunction:tablie1:{"+value1.getTable()+"},tablie2:{"+value2.getTable()+"}。"
                            + "size1:{"+value1.getData().size()+"},size2:{"+value2.getData().size()+"}。"
                            + "type1:{"+value1.getType()+"},type2:{"+value2.getType()+"}----------------------");
                        return value1.getData().size()>value2.getData().size() ? value1 : value2 ;
                    }});

            dataStream1.print();

            // 执行数据流
            env1.execute("geekplus_dws_etl_job1");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("error:" + e.getMessage());
        }
    }

}
