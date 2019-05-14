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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * FLINK的keyby算子的demo
 * 
 * 
 * @company GeekPlus
 * @project jaguar
 * @author qiunan
 * @date Apr 30, 2019
 * @since 1.0.0
 */
public class KeyByDemo {
    private static Logger logger = Logger.getLogger(KeyByDemo.class);

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
                .name("source1").setParallelism(1).map(new MapFunction<Map<String, String>, BinLogMsgEntity>() {
                    @Override
                    public BinLogMsgEntity map(Map<String, String> value) throws Exception {
                        BinLogMsgEntity msgEntity = JSON.parseObject(value.get(Constant.MQ_CONSTANT_ADDRESS),
                            new TypeReference<BinLogMsgEntity>() {});
                        return msgEntity;
                    }
                });

            // 3.keyby的多种写法
            // 3.1.通过自定义keySelect
            KeyedStream<BinLogMsgEntity, String> keyedStream1 =
                dataStream1.keyBy(new KeySelector<BinLogMsgEntity, String>() {

                    @Override
                    public String getKey(BinLogMsgEntity value) throws Exception {
                        // TODO Auto-generated method stub
                        return value.getTable();
                    }
                });
            // 3.2.通过pojo的字段名
            KeyedStream<BinLogMsgEntity, Tuple> keyedStream2 = dataStream1.keyBy("table");
            // 3.3.通过指定tuple
            KeyedStream<Tuple3<String, String, String>, Tuple> keyedStream3 =
                dataStream1.map(new MapFunction<BinLogMsgEntity, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(BinLogMsgEntity value) throws Exception {
                        Tuple3<String, String, String> tuple3 = new Tuple3<>();
                        tuple3.setFields(value.getTable(), value.getDatabase(), value.getSql());
                        return tuple3;
                    }
                }).keyBy(0);

            // 4执行每5秒时间窗口操作，计数
            SingleOutputStreamOperator<Map<String, Integer>>  o1 =keyedStream1.timeWindow(Time.seconds(5))
                .apply(new WindowFunction<BinLogMsgEntity, Map<String, Integer>, String, TimeWindow>() {

                    @Override
                    public void apply(String key, TimeWindow window, Iterable<BinLogMsgEntity> input,
                        Collector<Map<String, Integer>> out) throws Exception {
                        Map<String, Integer> result = new HashMap<>();
                        input.forEach(b -> {
                            int count = result.get(key) == null ? 0 : result.get(key);
                            count++;
                            result.put(key, count);
                        });
                        out.collect(result);
                    }
                });
            SingleOutputStreamOperator<Map<String, Integer>>  o2 =keyedStream2.timeWindow(Time.seconds(5))
                .apply(new WindowFunction<BinLogMsgEntity, Map<String, Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple key, TimeWindow window, Iterable<BinLogMsgEntity> input,
                        Collector<Map<String, Integer>> out) throws Exception {
                        Map<String, Integer> result = new HashMap<>();
                        input.forEach(b -> {
                            int count = result.get(key.toString()) == null ? 0 : result.get(key.toString());
                            count++;
                            result.put(key.toString(), count);
                        });
                        out.collect(result);
                    }
                });

            SingleOutputStreamOperator<Map<String, Integer>>  o3 = keyedStream3.timeWindow(Time.seconds(5))
                .apply(new WindowFunction<Tuple3<String, String, String>, Map<String, Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, String, String>> input,
                        Collector<Map<String, Integer>> out) throws Exception {
                        Map<String, Integer> result = new HashMap<>();
                        input.forEach(b -> {
                            int count = result.get(key.toString()) == null ? 0 : result.get(key.toString());
                            count++;
                            result.put(key.toString(), count);
                        });
                        out.collect(result);
                    }
                });
            o1.print();
            o2.print();
            o3.print();

            // 执行数据流
            env1.execute("geekplus_dws_etl_job1");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("error:" + e.getMessage());
        }
    }

}
