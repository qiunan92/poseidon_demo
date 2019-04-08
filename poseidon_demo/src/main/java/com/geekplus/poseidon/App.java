package com.geekplus.poseidon;


import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;


@SpringBootApplication
@MapperScan("com.geekplus.poseidon")
@EntityScan("com.geekplus.poseidon")
public class App {
    
//	public static void main(String[] args) {
//	    System.out.println("hello world!");
//	    SpringApplication.run(App.class, args);
//	}
}
