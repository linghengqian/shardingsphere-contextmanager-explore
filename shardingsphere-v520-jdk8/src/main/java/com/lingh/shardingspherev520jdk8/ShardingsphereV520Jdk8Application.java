package com.lingh.shardingspherev520jdk8;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.lingh.shardingspherev520jdk8.mapper")
public class ShardingsphereV520Jdk8Application {

    public static void main(String[] args) {
        SpringApplication.run(ShardingsphereV520Jdk8Application.class, args);
    }

}
