package com.qiunan.data_sync.flinkdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.qiunan.data_sync.rmqflink.RocketMQConfig;
import com.qiunan.data_sync.rmqflink.RocketMQSource;
import com.qiunan.data_sync.rmqflink.common.serialization.SimpleKeyValueDeserializationSchema;
import com.qiunan.data_sync.common.Constant;
import com.qiunan.data_sync.dws.entity.BinLogMsgEntity;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * FLINK的union算子的demo
 * 个人认为可以用在ods获取多个仓库的数据源时，在map进行写warehouse_id后，进行多个数据源的合并
 * 有个问题是多个数据源的情况下,consumer_group不能相同。。。
 * 
 * @company GeekPlus
 * @project jaguar
 * @author qiunan
 * @date Apr 30, 2019
 * @since 1.0.0
 */
public class UnionDemo {
    private static Logger logger = Logger.getLogger(UnionDemo.class);

    public static void main(String[] args) {
        try {
            // 1.初始化两个数据源
            StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();

            Properties consumerProps1 = new Properties();
            consumerProps1.setProperty(RocketMQConfig.NAME_SERVER_ADDR, Constant.SOURCE_NAME_SERVER_ADDR);
            consumerProps1.setProperty(RocketMQConfig.CONSUMER_GROUP, "flink_demo");
            consumerProps1.setProperty(RocketMQConfig.CONSUMER_TOPIC, "BinLogFromCanal");
            Properties consumerProps2 = new Properties();
            consumerProps2.setProperty(RocketMQConfig.NAME_SERVER_ADDR, Constant.SOURCE_NAME_SERVER_ADDR);
            consumerProps2.setProperty(RocketMQConfig.CONSUMER_GROUP, "flink_demo1");
            consumerProps2.setProperty(RocketMQConfig.CONSUMER_TOPIC, "MsgFromOds");

            // 2.初始化数据源,对数据源进行映射，过滤，根据表名分成多个侧数据流
            DataStream<String> dataStream1 = env1
                .addSource(new RocketMQSource(
                    new SimpleKeyValueDeserializationSchema(Constant.MQ_CONSTANT_ID, Constant.MQ_CONSTANT_ADDRESS),
                    consumerProps1))
                .name("source1").setParallelism(1)
                .map(new MapFunction<Map<String, String>, String>() {
                    @Override
                    public String map(Map<String, String> value) throws Exception {
                        StringBuffer str = new StringBuffer("source1:");
                        BinLogMsgEntity msgEntity = JSON.parseObject(value.get(Constant.MQ_CONSTANT_ADDRESS),
                            new TypeReference<BinLogMsgEntity>() {});
                        str.append(msgEntity.getDatabase());
                        return str.toString();
                    }
                });
            DataStream<String> dataStream2 = env1
                .addSource(new RocketMQSource(
                    new SimpleKeyValueDeserializationSchema(Constant.MQ_CONSTANT_ID, Constant.MQ_CONSTANT_ADDRESS),
                    consumerProps2))
                .name("source2").setParallelism(1)
                .map(new MapFunction<Map<String, String>, String>() {
                    @Override
                    public String map(Map<String, String> value) throws Exception {
                        StringBuffer str = new StringBuffer("source2:");
                        BinLogMsgEntity msgEntity = JSON.parseObject(value.get(Constant.MQ_CONSTANT_ADDRESS),
                            new TypeReference<BinLogMsgEntity>() {});
                        str.append(msgEntity.getDatabase());
                        return str.toString();
                    }
                });

            dataStream2.print();
            dataStream1.print();
            //3.将两个数据流合并
            DataStream<String> unionStream  = dataStream2.union(dataStream1).map(new MapFunction<String, String>() {

                @Override
                public String map(String value) throws Exception {
                    String str = value.replaceAll("source1", "unionSource").replaceAll("source2", "unionSource");
                    return str;
                }});
            
            //4.打印
            unionStream.print();
            
            //执行数据流
            env1.execute("geekplus_dws_etl_job1");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("error:" + e.getMessage());
        }
    }

}
