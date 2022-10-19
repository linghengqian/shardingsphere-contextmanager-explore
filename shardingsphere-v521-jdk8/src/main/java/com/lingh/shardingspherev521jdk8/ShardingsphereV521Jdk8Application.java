package com.lingh.shardingspherev521jdk8;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.lingh.shardingspherev521jdk8.mapper")
public class ShardingsphereV521Jdk8Application {

    public static void main(String[] args) {
        SpringApplication.run(ShardingsphereV521Jdk8Application.class, args);
    }

}
