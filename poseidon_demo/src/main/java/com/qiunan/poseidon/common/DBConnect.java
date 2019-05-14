package com.qiunan.poseidon.common;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class DBConnect {

    private final static String Driver="com.mysql.jdbc.Driver";
    private static Connection source;
    private static Connection target;


    public static synchronized  Connection getSourceConnection () throws SQLException {
        if(source == null || source.isClosed())
        {
            synchronized (DBConnect.class){
                if(source == null || source.isClosed()){
                    source= DriverManager.getConnection(Constant.SOURCE_DBURL, Constant.SOURCE_USERNAME, Constant.SOURCE_PASSWORD);
                }
            }
        }
        return source;
    }

    public static synchronized  Connection getTargetConnection () throws SQLException
    {
        if(target == null || target.isClosed())
        {
            synchronized (DBConnect.class){
                if(target == null || target.isClosed()){
                    target= DriverManager.getConnection(Constant.TARGET_DBURL, Constant.TARGET_USERNAME, Constant.TARGET_PASSWORD);
                }
            }
        }
        return target;
    }

    public static void executeSql(String sqlStr) throws SQLException {
        log.info("execute sql:{"+sqlStr+"}");
        PreparedStatement preparedStatement = getTargetConnection().prepareStatement(sqlStr);
        preparedStatement.execute();

    }
}
