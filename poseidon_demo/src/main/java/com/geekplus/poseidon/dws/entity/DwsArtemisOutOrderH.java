package com.geekplus.poseidon.dws.entity;

import java.io.Serializable;

import com.baomidou.mybatisplus.annotation.TableName;

import lombok.Data;

@TableName("dws_artemis_out_order_h")
@Data
public class DwsArtemisOutOrderH implements Serializable{
    /**  **/
    private static final long serialVersionUID = 1L;
    private Long id;
    private Integer year;
    private Integer month;
    private Integer day;
    private Integer hour;
    private Integer inputOrderCount;
}
