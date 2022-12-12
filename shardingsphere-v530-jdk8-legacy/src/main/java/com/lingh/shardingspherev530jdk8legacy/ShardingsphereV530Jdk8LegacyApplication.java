package com.lingh.shardingspherev530jdk8legacy;

import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.StreamUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

@SpringBootApplication
@MapperScan("com.lingh.shardingspherev530jdk8legacy.mapper")
public class ShardingsphereV530Jdk8LegacyApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShardingsphereV530Jdk8LegacyApplication.class, args);
    }

    @Bean
    DataSource shardingSphereDataSource() {
        try {
            // Since jdk 9, we should use `java.io.InputStream#readAllBytes` instead of `org.springframework.util.StreamUtils#copyToByteArray`
            byte[] bytes = StreamUtils.copyToByteArray(new ClassPathResource("config.yaml").getInputStream());
            return YamlShardingSphereDataSourceFactory.createDataSource(bytes);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
