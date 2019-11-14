package com.qiunan.data_sync.common;

public class Constant {
    // -----------------MQ source config ------------------//
    public final static String SOURCE_NAME_SERVER_ADDR = "10.44.50.244:9876;10.44.50.245:9876";
    public final static String SOURCE_CONSUMER_GROUP_ODS = "ods";
    public final static String SOURCE_CONSUMER_TOPIC = "test";
    public final static String TOPIC_MSG_FROM_DWD = "MsgFromDwd";
    public final static String SOURCE_CONSUMER_GROUP_DWD = "dwd";
    public final static String SOURCE_CONSUMER_GROUP_DWS = "dws";
    public final static String SOURCE_CONSUMER_GROUP_DM = "dm";

    // -----------------mq constant ------------------//
    public final static String MQ_CONSTANT_ID = "id";
    public final static String MQ_CONSTANT_ADDRESS = "address";

    public final static String MQ_MSG_ADDRESS = "address";
    public final static String MQ_MSG_TYPE_INSERT = "INSERT";
    public final static String MQ_MSG_TYPE_UPDATE = "UPDATE";
    
    // -----------------flink constant ------------------//
    public final static int FLINK_CHECKPOINTING = 3000;
    public final static String FLINK_SOURCE_NAME = "rocketmq-source";
    public final static int FLINK_SOURCE_PARALLELISM = 2;
    public final static String FLINK_PROCESS_NAME = "etl-processor";
    public final static int FLINK_PROCESS_PARALLELISM = 2;
    public final static String FLINK_SINK_NAME = "rocketmq-sink";
    public final static int FLINK_SINK_PARALLELISM = 2;
    public final static String FLINK_JOB_NAME = "geekplus_ods_etl_job";

    // -----------------sql constant ------------------//
    public final static String SQL_NEW_TABLE_PREFIX = "ods_";
    public final static String SQL_INSERT_HEADER = "INSERT INTO ";
    public final static String SQL_INSERT_VALUES = "VALUES";

    public final static String SQL_UPDATE = "UPDATE ";
    public final static String SQL_UPDATE_SET = " SET ";
    public final static String SQL_UPDATE_WHERE = " WHERE ";
    public final static String SQL_UPDATE_AND = " AND ";

    public final static String SQL_LEFT_BRACKETS = " ( ";
    public final static String SQL_RIGHT_BRACKETS = " ) ";
    public final static String SQL_COMMA = " , ";
    public final static String SQL_EQUAL_SIGN = " = ";

    // -----------------other constant ------------------//
    public final static String ESCAPE_CHARACTER_DOUBLE_QUOTATION_MARKS = "\"";
}
