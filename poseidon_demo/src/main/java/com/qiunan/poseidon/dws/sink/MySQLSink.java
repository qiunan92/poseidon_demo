package com.qiunan.poseidon.dws.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;

public class MySQLSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;
    private static Logger logger = Logger.getLogger(MySQLSink.class);






    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
    }

    @Override
    public void invoke(String sqlStr) throws Exception {
//        PreparedStatement preparedStatement = DBConnect.getTargetConnection().prepareStatement(sqlStr);
//        preparedStatement.execute();
//        logger.info("execute sql:{"+sqlStr+"}");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}
