package com.geekplus.poseidon.dws.process;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.geekplus.poseidon.App;
import com.geekplus.poseidon.common.Constant;
import com.geekplus.poseidon.dws.entity.BinLogMsgEntity;
import com.geekplus.poseidon.dws.entity.DwsArtemisOutOrderH;
import com.geekplus.poseidon.dws.mapper.DwsArtemisOutOrderHMapper;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DwsSQLPocessFunction extends ProcessFunction<BinLogMsgEntity, String>  {

    /**  **/
    private static final long serialVersionUID = 1L;
    private ApplicationContext applicationContext;
    private DwsArtemisOutOrderHMapper dwsArtemisOutOrderHMapper;
    

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 在这里加载spring的上下文
        if (applicationContext == null) {
            this.applicationContext= SpringApplication.run(App.class);
            this.dwsArtemisOutOrderHMapper = (DwsArtemisOutOrderHMapper)applicationContext.getBean("dwsArtemisOutOrderHMapper");
        }
    }

    @Override
    public void processElement(BinLogMsgEntity msgEntity, Context arg1, Collector<String> out) {
        try {
            // 1.判断这条数据是否为我们需要的数据
            if (!"out_order".equals(msgEntity.getTable())) {
                return;
            }

            // 2.判断是否为ddl
            if (msgEntity.isDdl()) {
                // ddl语句暂时忽略
                // processDdl(msgEntity, newTable, out);
            } else {
                // 3.根据dml类型进行处理
                if (StringUtils.equals(Constant.MQ_MSG_TYPE_INSERT, msgEntity.getType())) {
                    Map<String, Integer> timeZoneCountMap = new HashMap<>();
                    msgEntity.getData().forEach(d -> {
                        String times = null;
                        if (StringUtils.isNotEmpty(d.getString("input_date"))
                            && !StringUtils.equals("0", d.getString("input_date"))) {
                            times = getHourTimesByStringValue(d.getString("input_date"));
                        } else {
                            times = getHourTimesByStringValue(msgEntity.getEs().toString());
                        }
                        if (StringUtils.isNotEmpty(times)) {
                            Integer count = timeZoneCountMap.get(times) == null ? 0 : timeZoneCountMap.get(times);
                            count++;
                            timeZoneCountMap.put(times, count);
                        }

                    });
                    Iterator<String> it = timeZoneCountMap.keySet().iterator();
                    while (it.hasNext()) {
                        String timeZone = it.next();
                        Timestamp timeZoneTs= Timestamp.valueOf(timeZone);
                        Integer count = timeZoneCountMap.get(timeZone);
                        //查询出当前时间段是否存在数据
                        QueryWrapper<DwsArtemisOutOrderH> qw = new QueryWrapper<>();
//                        LambdaQueryWrapper<DwsArtemisOutOrderH>lambdaQueryWrapper= new LambdaQueryWrapper<>();
//                        lambdaQueryWrapper.eq(DwsArtemisOutOrderH::getYear, timeZoneTs.getYear());
                        qw.eq("year", timeZoneTs.getYear());
                        qw.eq("month", timeZoneTs.getMonth());
                        qw.eq("day", timeZoneTs.getDay());
                        qw.eq("hour", timeZoneTs.getHours());
                        DwsArtemisOutOrderH queryResult = dwsArtemisOutOrderHMapper.selectOne(qw);
                        
                        //存在则更新
                        if(null != queryResult) {
                            queryResult.setInputOrderCount(queryResult.getInputOrderCount()+count);
                            UpdateWrapper<DwsArtemisOutOrderH> uw = new UpdateWrapper<>();
                            uw.eq("id", queryResult.getId());
                            dwsArtemisOutOrderHMapper.updateById(queryResult);
                        }else {
                            DwsArtemisOutOrderH insertEntity = new DwsArtemisOutOrderH();
                            insertEntity.setYear(timeZoneTs.getYear());
                            insertEntity.setMonth(timeZoneTs.getMonth());
                            insertEntity.setDay(timeZoneTs.getDay());
                            insertEntity.setHour(timeZoneTs.getHours());
                            insertEntity.setInputOrderCount(count);
                            dwsArtemisOutOrderHMapper.insert(insertEntity);
                        }
                    }

                } else {
                    // 暂时不考虑
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static String getHourTimesByStringValue(String value) {
        if (StringUtils.isNotEmpty(value)) {
            if (value.length() == 13) {
                Long oldValue = Long.parseLong(value);
                Long newValue = oldValue / 3600000 * 3600000;
                return new Timestamp(newValue).toString();
            } else if (value.length() == 10) {
                Long oldValue = Long.parseLong(value);
                Long newValue = oldValue / 3600 * 3600000;
                return new Timestamp(newValue).toString();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

}
