package com.geekplus.poseidon.dws;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import com.geekplus.poseidon.App;
import com.geekplus.poseidon.common.Constant;
import com.geekplus.poseidon.dws.entity.BinLogMsgEntity;
import com.geekplus.poseidon.dws.mapper.DwsArtemisOutOrderHMapper;
import com.geekplus.poseidon.dws.process.DwsSQLPocessFunction;
import com.geekplus.poseidon.rmqflink.RocketMQConfig;
import com.geekplus.poseidon.rmqflink.RocketMQSource;
import com.geekplus.poseidon.rmqflink.common.serialization.SimpleKeyValueDeserializationSchema;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

public class DwsETLJob {
    private static final Logger LOG = LoggerFactory.getLogger(DwsETLJob.class);

    @SuppressWarnings({"unchecked", "rawtypes", "serial"})
    public static void main(String[] args) {
        try {
            System.out.println("start job");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().disableSysoutLogging();
            // enable checkpoint
            env.enableCheckpointing(Constant.FLINK_CHECKPOINTING);
            env.getConfig().disableSysoutLogging();

            Properties consumerProps = new Properties();
            consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, Constant.SOURCE_NAME_SERVER_ADDR);
            consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, Constant.SOURCE_CONSUMER_GROUP_DWS);
            consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, Constant.SOURCE_CONSUMER_TOPIC);

            env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema(Constant.MQ_CONSTANT_ID, 
                Constant.MQ_CONSTANT_ADDRESS),consumerProps))
                .name(Constant.FLINK_SOURCE_NAME)
                .setParallelism(Constant.FLINK_SOURCE_PARALLELISM)
                .map(new MapFunction<Map, BinLogMsgEntity>() {

                    @Override
                    public BinLogMsgEntity map(Map value) throws Exception {
                        BinLogMsgEntity msgEntity =
                            JSON.parseObject((String)value.get(Constant.MQ_MSG_ADDRESS), new TypeReference<BinLogMsgEntity>() {});
                        return msgEntity;
                    }
                })
                .process(new DwsSQLPocessFunction())
                .name(Constant.FLINK_PROCESS_NAME)
                .setParallelism(Constant.FLINK_PROCESS_PARALLELISM);
            
            env.execute(Constant.FLINK_JOB_NAME);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

}
