package com.qiunan.poseidon.dws.transfer;

//import com.alibaba.fastjson.JSONObject;
//import com.geekplus.warehouse.common.DBConnect;
//import com.geekplus.warehouse.common.Utils;
//import com.geekplus.warehouse.entity.BaseMsgEntity;
//import com.geekplus.warehouse.entity.BinLogMsgEntity;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 收到一条 out_order 的 MQ 消息后，要把这条消息转换成怎样的 SQL 在 DWS 执行
 */
public class DwsOutOrderTransfer
//implements DwsBaseTransfer 
{


//    /**
//     * insert 消息，判断在 dws 对应的表中是否存在
//     * 不存在， create， input_num = 1
//     * 存在，   update set  input_num = input_num + 1
//     */
//    public List<String> insertSQL(BaseMsgEntity msg) {
//        List<String> result = new ArrayList<>();
//        BinLogMsgEntity msgEntity = (BinLogMsgEntity) msg;
//        for(JSONObject jo : msgEntity.getData()){
//            long inputTS = jo.getLong("input_date");
//            int[] ymdh = Utils.getYYYYMMDDHHFromTs(inputTS);
//            java.lang.String selectSQL = "SELECT * from dws.dws_order_propower_input where year = " + ymdh[0]  +
//                    " and month = " + ymdh[1] +
//                    " and day = " + ymdh[2] +
//                    " and hour = " +  ymdh[3];
//            try {
//                PreparedStatement preparedStatement = DBConnect.getTargetConnection().prepareStatement(selectSQL);
//                ResultSet resSet = preparedStatement.executeQuery();
//
//                if (!resSet.next()) {
//                    String insertSQL = "INSERT INTO dws.dws_order_propower_input (year, month, day, hour, input_num) VALUES (" +
//                             + ymdh[0] + "," + ymdh[1] + "," + ymdh[2] + "," + ymdh[3] + "," + 1 + ")";
//                    result.add(insertSQL);
//                }
//                else {
//                    String updateSQL = "UPDATE dws.dws_order_propower_input SET input_num = input_num + 1 WHERE year = " + ymdh[0]  +
//                            " and month = " + ymdh[1] +
//                            " and day = " + ymdh[2] +
//                            " and hour = " +  ymdh[3];
//                    result.add(updateSQL);
//                }
//            }catch(SQLException e){
//                e.printStackTrace();
//            }
//        }
//        return result;
//
//    }
//
//    @Override
//    public List<String> updateSQL(BaseMsgEntity msg) {
//        return new ArrayList<String>();
//    }
}
