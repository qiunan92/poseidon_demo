package com.geekplus.poseidon.demo;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.qiunan.poseidon.dws.entity.DwsArtemisOutOrderH;
import com.qiunan.poseidon.dws.mapper.DwsArtemisOutOrderHMapper;
import com.qiunan.poseidon.dws.service.IDwsArtemisOutOrderHService;

@RunWith(SpringRunner.class)
@SpringBootTest
public class AppTests {

    @Autowired
    private  DwsArtemisOutOrderHMapper dwsArtemisOutOrderHMapper;
    
    @Autowired
    @Qualifier(value="dwsArtemisOutOrderHServiceImpl") 
    IDwsArtemisOutOrderHService iDwsArtemisOutOrderHService;
    
	@Test
	public void contextLoads() {
	    try {
	        List<DwsArtemisOutOrderH> list =dwsArtemisOutOrderHMapper.selectList(null);
	        list.forEach(d ->{
	            System.out.println(d.getId());
	        });
	        List<DwsArtemisOutOrderH> list1= iDwsArtemisOutOrderHService.list();
	        list1.forEach(d ->{
                System.out.println(d.getId());
            });
	        
	        QueryWrapper<DwsArtemisOutOrderH> qw = new QueryWrapper<>();
            qw.eq("year", 2019);
            qw.eq("month", 4);
            qw.eq("day", 2);
            qw.eq("hour", 17);
            DwsArtemisOutOrderH queryResult = dwsArtemisOutOrderHMapper.selectOne(qw);
	      
            System.out.println(queryResult.getId());
        } catch (Exception e) {
           e.printStackTrace();
        }
	    
	}

}
