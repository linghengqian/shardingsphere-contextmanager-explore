package com.lingh.shardingspherev530jdk8legacy.pojo;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TOrderShardingSpherePO {
    private Long id;
    private LocalDateTime createTime;
    private String comment;
}
