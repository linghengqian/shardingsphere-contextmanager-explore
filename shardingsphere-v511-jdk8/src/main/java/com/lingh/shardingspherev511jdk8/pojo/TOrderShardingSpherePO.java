package com.lingh.shardingspherev511jdk8.pojo;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TOrderShardingSpherePO {
    private Long id;
    private LocalDateTime createTime;
    private String comment;
}
