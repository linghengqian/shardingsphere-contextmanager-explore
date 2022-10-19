package com.lingh.shardingspherev512jdk8.mapper;

import com.lingh.shardingspherev512jdk8.pojo.TOrderShardingSpherePO;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;

@SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection"})
@Component
public interface TOrderShardingSphereMapper {

    @Select("select * from t_order_sharding_sphere")
    List<TOrderShardingSpherePO> findAll();
}
