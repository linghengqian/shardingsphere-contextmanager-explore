package com.lingh.shardingspherev511jdk8;

import org.junit.jupiter.api.Test;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@MapperScan("com.lingh.shardingspherev511jdk8.mapper")
class ShardingsphereV511Jdk8ApplicationTests {

    @Test
    void contextLoads() {
        System.out.println("Aloha, World!");
    }

}
