package com.geekplus.poseidon.dws.entity;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class BinLogMsgEntity implements Serializable {
    /**  **/
    private static final long serialVersionUID = 1L;

    @JSONField
    private List<JSONObject> data;
    
    @JSONField
    private String database;
    
    @JSONField
    private Long es;
    
    @JSONField
    private Long id;
    
    @JSONField
    private boolean isDdl;
    
    @JSONField
    private String table;
    
    @JSONField
    private Long ts;
    
    @JSONField
    private String type;
    
    @JSONField
    private List<JSONObject> mysqlType;
    
    @JSONField
    private List<JSONObject> old;
    
    @JSONField
    private JSONObject sqlType;
    
    @JSONField
    private String sql;

   

}
