package com.lingh.shardingspherev530jdk8modern;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.lingh.shardingspherev530jdk8modern.mapper")
public class ShardingsphereV530Jdk8ModernApplication {

	public static void main(String[] args) {
		SpringApplication.run(ShardingsphereV530Jdk8ModernApplication.class, args);
	}

}
