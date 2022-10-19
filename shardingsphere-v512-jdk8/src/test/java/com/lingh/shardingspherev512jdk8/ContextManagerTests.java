package com.lingh.shardingspherev512jdk8;

import com.lingh.shardingspherev512jdk8.mapper.TOrderShardingSphereMapper;
import com.lingh.shardingspherev512jdk8.utils.LocalShardingDatabasesAndTablesUtil;
import lombok.SneakyThrows;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.sharding.algorithm.config.AlgorithmProvidedShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.algorithm.sharding.datetime.IntervalShardingAlgorithm;
import org.apache.shardingsphere.sharding.spi.ShardingAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class ContextManagerTests {
    @Resource
    private ShardingSphereDataSource shardingSphereDataSource;
    @Autowired
    TOrderShardingSphereMapper tOrderShardingSphereMapper;
    
    
    @BeforeEach
    void before(){
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
        assertEquals(tOrderShardingSphereMapper.findAll().size(), 2);
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
        assertEquals(tOrderShardingSphereMapper.findAll().size(), 3);
    }

    @Test
    void whenRequestToUpdateActualDataNodesByJupiter() {
        String newActualDataNodes = "ds-0.t_order_$->{20221010..20221012}";
        String oldLogicTableName = "t_order_sharding_sphere";
        String oldDatabaseName = "sharding_db";
        LocalShardingDatabasesAndTablesUtil.updateActualDataNodesByJupiter(
                shardingSphereDataSource, oldDatabaseName, oldLogicTableName, newActualDataNodes
        );
        assertEquals(LocalShardingDatabasesAndTablesUtil.getActualDataNodesByVintage(
                        shardingSphereDataSource, oldDatabaseName, oldLogicTableName
                ),
                "ds-0.t_order_$->{20221010..20221012}");
        assertEquals(tOrderShardingSphereMapper.findAll().size(), 3);
    }

    @Test
    void whenRequestToRecreateAlgorithmConfiguration() {
        String oldDatabaseName = "sharding_db";
        String oldLogicTableName = "t_order_sharding_sphere";
        ContextManager contextManager = getContextManager(shardingSphereDataSource);
        Collection<RuleConfiguration> newRuleConfigList = new LinkedList<>();
        Collection<RuleConfiguration> oldRuleConfigList = contextManager
                .getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(oldDatabaseName)
                .getRuleMetaData()
                .getConfigurations();
        oldRuleConfigList.stream()
                .filter(oldRuleConfig -> oldRuleConfig instanceof AlgorithmProvidedShardingRuleConfiguration)
                .map(oldRuleConfig -> (AlgorithmProvidedShardingRuleConfiguration) oldRuleConfig)
                .forEach(oldAlgorithmConfig -> {
                    Supplier<ShardingAlgorithm> shardingAlgorithmSupplier = () -> {
                        Properties props = new Properties();
                        props.setProperty("datetime-pattern", "yyyy-MM-dd HH:mm:ss.SSS");
                        props.setProperty("datetime-lower", "2022-10-10 00:00:00.000");
                        props.setProperty("datetime-upper", "2022-10-12 23:59:59.999");
                        props.setProperty("sharding-suffix-pattern", "_yyyyMMdd");
                        props.setProperty("datetime-interval-amount", "1");
                        props.setProperty("datetime-interval-unit", "DAYS");
                        IntervalShardingAlgorithm intervalShardingAlgorithm = new IntervalShardingAlgorithm();
                        intervalShardingAlgorithm.init(props);
                        return intervalShardingAlgorithm;
                    };
                    oldAlgorithmConfig.getShardingAlgorithms().put("lingh-interval", shardingAlgorithmSupplier.get());
                    newRuleConfigList.add(oldAlgorithmConfig);
                });
        contextManager.alterRuleConfiguration(oldDatabaseName, newRuleConfigList);
        assertEquals(LocalShardingDatabasesAndTablesUtil.getActualDataNodesByVintage(
                        shardingSphereDataSource, oldDatabaseName, oldLogicTableName
                ),
                "ds-0.t_order_$->{20221010..20221011}");
    }

    @SneakyThrows(ReflectiveOperationException.class)
    private static ContextManager getContextManager(final ShardingSphereDataSource dataSource) {
        Field field = ShardingSphereDataSource.class.getDeclaredField("contextManager");
        field.setAccessible(true);
        return (ContextManager) field.get(dataSource);
    }
}
