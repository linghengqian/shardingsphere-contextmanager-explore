package com.lingh.shardingspherev530jdk8modern.mapper;

import com.lingh.shardingspherev530jdk8modern.pojo.TOrderShardingSpherePO;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

@SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection"})
@Component
public interface TOrderShardingSphereMapper {

    @Select("select * from t_order_sharding_sphere")
    List<TOrderShardingSpherePO> findAll();
}
