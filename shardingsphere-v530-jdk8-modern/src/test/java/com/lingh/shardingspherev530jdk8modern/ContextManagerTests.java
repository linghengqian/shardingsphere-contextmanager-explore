package com.lingh.shardingspherev530jdk8modern;

import com.lingh.shardingspherev530jdk8modern.mapper.TOrderShardingSphereMapper;
import com.lingh.shardingspherev530jdk8modern.utils.LocalShardingDatabasesAndTablesUtil;
import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.metadata.database.rule.ShardingSphereRuleMetaData;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class ContextManagerTests {
    @Resource
    private DataSource shardingSphereDataSource;
    @Autowired
    TOrderShardingSphereMapper tOrderShardingSphereMapper;


    @BeforeEach
    void before() {
        String newActualDataNodes = "ds-0.t_order_$->{20221010..20221011}";
        String oldLogicTableName = "t_order_sharding_sphere";
        String oldDatabaseName = "sharding_db";
        LocalShardingDatabasesAndTablesUtil.updateActualDataNodesByJupiter(
                shardingSphereDataSource, oldDatabaseName, oldLogicTableName, newActualDataNodes
        );
    }

    @Test
    void whenRequestToGetActualDataNodesByVintage() {
        String oldLogicTableName = "t_order_sharding_sphere";
        String oldDatabaseName = "sharding_db";
        assertEquals("ds-0.t_order_$->{20221010..20221011}",
                LocalShardingDatabasesAndTablesUtil.getActualDataNodesByVintage(
                        shardingSphereDataSource, oldDatabaseName, oldLogicTableName
                ));
        assertEquals(2, tOrderShardingSphereMapper.findAll().size());
    }

    @Test
    void whenRequestToUpdateActualDataNodesByJupiter() {
        String newActualDataNodes = "ds-0.t_order_$->{20221010..20221012}";
        String oldLogicTableName = "t_order_sharding_sphere";
        String oldDatabaseName = "sharding_db";
        LocalShardingDatabasesAndTablesUtil.updateActualDataNodesByJupiter(
                shardingSphereDataSource, oldDatabaseName, oldLogicTableName, newActualDataNodes
        );
        assertEquals(newActualDataNodes, LocalShardingDatabasesAndTablesUtil.getActualDataNodesByVintage(
                shardingSphereDataSource, oldDatabaseName, oldLogicTableName
        ));
        assertEquals(3, tOrderShardingSphereMapper.findAll().size());
    }

    @Test
    void whenRequestToRecreateAlgorithmConfiguration() {
        String oldDatabaseName = "sharding_db";
        String oldLogicTableName = "t_order_sharding_sphere";
        ContextManager contextManager = getContextManager(shardingSphereDataSource);
        ShardingSphereRuleMetaData ruleMetaData = contextManager.getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(oldDatabaseName)
                .getRuleMetaData();
        Optional<ShardingRule> singleRule = ruleMetaData.findSingleRule(ShardingRule.class);
        assert singleRule.isPresent();
        ShardingRuleConfiguration currentRuleConfig = (ShardingRuleConfiguration) singleRule.get().getConfiguration();
        Supplier<AlgorithmConfiguration> algorithmConfigurationSupplier = () -> {
            Properties props = new Properties();
            props.setProperty("datetime-pattern", "yyyy-MM-dd HH:mm:ss.SSS");
            props.setProperty("datetime-lower", "2022-10-10 00:00:00.000");
            props.setProperty("datetime-upper", "2022-10-12 23:59:59.999");
            props.setProperty("sharding-suffix-pattern", "_yyyyMMdd");
            props.setProperty("datetime-interval-amount", "1");
            props.setProperty("datetime-interval-unit", "DAYS");
            return new AlgorithmConfiguration("INTERVAL", props);
        };
        currentRuleConfig.getShardingAlgorithms().put("lingh-interval", algorithmConfigurationSupplier.get());
        Collection<RuleConfiguration> toBeAlteredRuleConfigList = new LinkedList<>();
        toBeAlteredRuleConfigList.add(currentRuleConfig);
        ruleMetaData.getRules().stream().filter(shardingSphereRule -> !(shardingSphereRule instanceof ShardingRule))
                .forEach(shardingSphereRule -> toBeAlteredRuleConfigList.add(shardingSphereRule.getConfiguration()));
        contextManager.alterRuleConfiguration(oldDatabaseName, toBeAlteredRuleConfigList);
        assertEquals("ds-0.t_order_$->{20221010..20221011}",
                LocalShardingDatabasesAndTablesUtil.getActualDataNodesByVintage(
                        shardingSphereDataSource, oldDatabaseName, oldLogicTableName
                ));
    }

    private static ContextManager getContextManager(final DataSource dataSource) {
        try (ShardingSphereConnection connection = dataSource.getConnection().unwrap(ShardingSphereConnection.class)) {
            return connection.getContextManager();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
