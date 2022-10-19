package com.lingh.shardingspherev511jdk8;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.lingh.shardingspherev511jdk8.mapper")
public class ShardingsphereV511Jdk8Application {

    public static void main(String[] args) {
        SpringApplication.run(ShardingsphereV511Jdk8Application.class, args);
    }

}
