package com.qiunan.poseidon.flinkdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.qiunan.poseidon.common.Constant;
import com.qiunan.poseidon.dws.entity.BinLogMsgEntity;
import com.qiunan.poseidon.rmqflink.RocketMQConfig;
import com.qiunan.poseidon.rmqflink.RocketMQSource;
import com.qiunan.poseidon.rmqflink.common.serialization.SimpleKeyValueDeserializationSchema;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * FLINK的connect算子的demo
 * 
 * @company GeekPlus
 * @project jaguar
 * @author qiunan
 * @date Apr 30, 2019
 * @since 1.0.0
 */
public class ConnectedDemo {
    private static Logger logger = Logger.getLogger(ConnectedDemo.class);

    private static final OutputTag<String> table = new OutputTag<String>("table") {};
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
            DataStream<Tuple2<String,String>> dataStream1 = env1
                .addSource(new RocketMQSource(
                    new SimpleKeyValueDeserializationSchema(Constant.MQ_CONSTANT_ID, Constant.MQ_CONSTANT_ADDRESS),
                    consumerProps1))
                .name("source1").setParallelism(1)
                .map(new MapFunction<Map<String, String>, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String,String> map(Map<String, String> value) throws Exception {
                        Tuple2<String,String> map = new Tuple2<>();
                        BinLogMsgEntity msgEntity = JSON.parseObject(value.get(Constant.MQ_CONSTANT_ADDRESS),
                            new TypeReference<BinLogMsgEntity>() {});
                        map.setFields(msgEntity.getTable(), msgEntity.getType());
                        return map;
                    }
                });
            
            DataStream<Tuple2<String,Long>> dataStream2 = env1
                .addSource(new RocketMQSource(
                    new SimpleKeyValueDeserializationSchema(Constant.MQ_CONSTANT_ID, Constant.MQ_CONSTANT_ADDRESS),
                    consumerProps2))
                .name("source2").setParallelism(1)
                .map(new MapFunction<Map<String, String>, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String,Long> map(Map<String, String> value) throws Exception {
                        Tuple2<String,Long> map = new Tuple2<>();
                        BinLogMsgEntity msgEntity = JSON.parseObject(value.get(Constant.MQ_CONSTANT_ADDRESS),
                            new TypeReference<BinLogMsgEntity>() {});
                        map.setFields(msgEntity.getTable(), msgEntity.getEs());
                        return map;
                    }
                });
            
            //3.connect 
            SingleOutputStreamOperator<String> connectedStream  = dataStream1.connect(dataStream2)
                .process(new CoProcessFunction<Tuple2<String,String>, Tuple2<String,Long>, String>() {
                //connect的process可以分别对两个流进行不同的处理，并且在处理的过程中，可以通过context写入侧数据流中。
                @Override
                public void processElement1(Tuple2<String, String> input1,
                    CoProcessFunction<Tuple2<String, String>, Tuple2<String, Long>, String>.Context context1,
                    Collector<String> output1) throws Exception {
//                    System.out.println("------processElement1:table:"+input1.f0+"；type:"+input1.f1+"。----------------");
//                    System.out.println("------processElement1:context1:"+context1.toString());
                    //可以通过CoProcessFunction往侧数据流里写数据
//                    System.out.println("------processElement1:currentProcessingTime:"+context1.timerService().currentProcessingTime()+";currentWatermark:"+context1.timerService().currentWatermark()+"。----------------");
//                    context1.output(table, input1.f0+input1.f1);
                    output1.collect("------processElement1:table:"+input1.f0+"；type:"+input1.f1+"。----------------");
                }

                @Override
                public void processElement2(Tuple2<String, Long> input2,
                    CoProcessFunction<Tuple2<String, String>, Tuple2<String, Long>, String>.Context context2,
                    Collector<String> output2) throws Exception {
//                    System.out.println("------processElement2:table:"+input2.f0+"；es:"+input2.f1+"。----------------");
//                    System.out.println("------processElement2:context2:"+context2.toString());
//                    context2.output(table, input2.f0+input2.f1);
                    output2.collect("------processElement2:table:"+input2.f0+"；es:"+input2.f1+"。----------------");
                }});
//            DataStream<String> outOrderStream = connectedStream.getSideOutput(table);
//            outOrderStream.print();
//            ConnectedStreams<Tuple2<String, String>, Tuple2<String, Long>> keyedConnectedStream = dataStream1.connect(dataStream2).keyBy(0,0);
            
            //4.打印
            connectedStream.print();
            
            //执行数据流
            env1.execute("geekplus_dws_etl_job1");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("error:" + e.getMessage());
        }
    }

}
 