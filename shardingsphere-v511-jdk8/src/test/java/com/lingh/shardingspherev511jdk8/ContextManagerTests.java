package com.lingh.shardingspherev511jdk8;

import com.lingh.shardingspherev511jdk8.mapper.TOrderShardingSphereMapper;
import com.lingh.shardingspherev511jdk8.utils.LocalShardingDatabasesAndTablesUtil;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class ContextManagerTests {
    @Resource
    private ShardingSphereDataSource shardingSphereDataSource;
    @Autowired
    TOrderShardingSphereMapper tOrderShardingSphereMapper;


    @BeforeEach
    void before() {
        String newActualDataNodes = "ds-0.t_order_$->{20221010..20221011}";
        String oldLogicTableName = "t_order_sharding_sphere";
        String oldDatabaseName = "sharding_db";
        LocalShardingDatabasesAndTablesUtil.updateActualDataNodesByVintage(
                shardingSphereDataSource, oldDatabaseName, oldLogicTableName, newActualDataNodes
        );
    }

    @Test
    void whenRequestToGetActualDataNodesByVintage() {
        String oldLogicTableName = "t_order_sharding_sphere";
        String oldDatabaseName = "sharding_db";
        assertEquals(LocalShardingDatabasesAndTablesUtil.getActualDataNodesByVintage(
                        shardingSphereDataSource, oldDatabaseName, oldLogicTableName
                ),
                "ds-0.t_order_$->{20221010..20221011}");
        assertEquals(2, tOrderShardingSphereMapper.findAll().size());
    }

    @Test
    void whenRequestToUpdateActualDataNodesByVintage() {
        String newActualDataNodes = "ds-0.t_order_$->{20221010..20221012}";
        String oldLogicTableName = "t_order_sharding_sphere";
        String oldDatabaseName = "sharding_db";
        LocalShardingDatabasesAndTablesUtil.updateActualDataNodesByVintage(
                shardingSphereDataSource, oldDatabaseName, oldLogicTableName, newActualDataNodes
        );
        assertEquals(LocalShardingDatabasesAndTablesUtil.getActualDataNodesByVintage(
                        shardingSphereDataSource, oldDatabaseName, oldLogicTableName
                ),
                "ds-0.t_order_$->{20221010..20221012}");
        assertEquals(3, tOrderShardingSphereMapper.findAll().size());
    }
}
