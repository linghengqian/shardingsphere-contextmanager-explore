package com.lingh.shardingspherev530jdk8modern;

import com.lingh.shardingspherev530jdk8modern.mapper.TOrderShardingSphereMapper;
import com.lingh.shardingspherev530jdk8modern.utils.LocalShardingDatabasesAndTablesUtil;
import com.lingh.shardingspherev530jdk8modern.utils.PropertiesBuilder;
import org.apache.shardingsphere.distsql.parser.segment.AlgorithmSegment;
import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.rule.ShardingSphereRuleMetaData;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingAutoTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.sharding.distsql.handler.update.AlterShardingTableRuleStatementUpdater;
import org.apache.shardingsphere.sharding.distsql.parser.segment.strategy.KeyGenerateStrategySegment;
import org.apache.shardingsphere.sharding.distsql.parser.segment.strategy.ShardingStrategySegment;
import org.apache.shardingsphere.sharding.distsql.parser.segment.table.AutoTableRuleSegment;
import org.apache.shardingsphere.sharding.distsql.parser.segment.table.TableRuleSegment;
import org.apache.shardingsphere.sharding.distsql.parser.statement.AlterShardingTableRuleStatement;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
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

    @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
    @Test
    void testMultipleUpdates() throws SQLException {
        IntStream.range(0, 20).forEach(i -> {
            String newActualDataNodes = "ds-0.t_order_$->{20221010..20221012}";
            String oldLogicTableName = "t_order_sharding_sphere";
            String oldDatabaseName = "sharding_db";
            LocalShardingDatabasesAndTablesUtil.updateActualDataNodesByJupiter(
                    shardingSphereDataSource, oldDatabaseName, oldLogicTableName, newActualDataNodes
            );
        });
        try (Connection connection = shardingSphereDataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement("select * from t_order_sharding_sphere")) {
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet).isNotNull();
        }
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
        contextManager.getMetaDataContexts().getPersistService().getDatabaseRulePersistService().persist(oldDatabaseName, toBeAlteredRuleConfigList);
        contextManager.reloadDatabaseMetaData(oldDatabaseName);
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

    @Test
    void testTemp(){
        ContextManager contextManager = getContextManager(shardingSphereDataSource);
        ShardingSphereDatabase database = contextManager.getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get("sharding_db");
        ShardingSphereRuleMetaData ruleMetaData = database.getRuleMetaData();
        Optional<ShardingRule> singleRule = ruleMetaData.findSingleRule(ShardingRule.class);
        assert singleRule.isPresent();
        ShardingRuleConfiguration currentRuleConfig = (ShardingRuleConfiguration) singleRule.get().getConfiguration();
        AlterShardingTableRuleStatementUpdater updater = new AlterShardingTableRuleStatementUpdater();
        AlterShardingTableRuleStatement statement = new AlterShardingTableRuleStatement(Arrays.asList(
                createCompleteAutoTableRule("t_order_item"), createCompleteTableRule("t_order")
        ));
        updater.checkSQLStatement(database, statement, currentRuleConfig);
        ShardingRuleConfiguration toBeAlteredRuleConfig = updater.buildToBeAlteredRuleConfiguration(statement);
        updater.updateCurrentRuleConfiguration(currentRuleConfig, toBeAlteredRuleConfig);
        assert currentRuleConfig.getTables().size() == 1;
        ShardingTableRuleConfiguration tableRule = currentRuleConfig.getTables().iterator().next();
        assert tableRule.getLogicTable().equals("t_order_item");
        assert tableRule.getActualDataNodes().equals("ds_${0..1}.t_order${0..1}");
        assert tableRule.getTableShardingStrategy() instanceof StandardShardingStrategyConfiguration;
        assert ((StandardShardingStrategyConfiguration) tableRule.getTableShardingStrategy()).getShardingColumn().equals("product_id");
        assert tableRule.getTableShardingStrategy().getShardingAlgorithmName().equals("t_order_item_table_core.standard.fixture");
        assert tableRule.getDatabaseShardingStrategy() instanceof StandardShardingStrategyConfiguration;
        assert tableRule.getDatabaseShardingStrategy().getShardingAlgorithmName().equals("t_order_item_database_inline");
        assert currentRuleConfig.getTables().size() == 1;
        ShardingAutoTableRuleConfiguration autoTableRule = currentRuleConfig.getAutoTables().iterator().next();
        assert autoTableRule.getLogicTable().equals("t_order");
        assert autoTableRule.getActualDataSources().equals("ds_0,ds_1");
        assert autoTableRule.getShardingStrategy().getShardingAlgorithmName().equals("t_order_foo.distsql.fixture");
        assert ((StandardShardingStrategyConfiguration) autoTableRule.getShardingStrategy()).getShardingColumn().equals("order_id");
        assert autoTableRule.getKeyGenerateStrategy().getColumn().equals("product_id");
        assert autoTableRule.getKeyGenerateStrategy().getKeyGeneratorName().equals("t_order_distsql.fixture");
    }

    private static AutoTableRuleSegment createCompleteAutoTableRule(final String logicTableName) {
        AutoTableRuleSegment result = new AutoTableRuleSegment(logicTableName, Arrays.asList("ds_0", "ds_1"));
        result.setKeyGenerateStrategySegment(new KeyGenerateStrategySegment("product_id", new AlgorithmSegment("DISTSQL.FIXTURE", new Properties())));
        result.setShardingColumn("order_id");
        result.setShardingAlgorithmSegment(new AlgorithmSegment("FOO.DISTSQL.FIXTURE", PropertiesBuilder.build(new PropertiesBuilder.Property("", ""))));
        return result;
    }

    private static TableRuleSegment createCompleteTableRule(final String logicTableName) {
        KeyGenerateStrategySegment keyGenerator = new KeyGenerateStrategySegment("product_id", new AlgorithmSegment("DISTSQL.FIXTURE", new Properties()));
        TableRuleSegment result = new TableRuleSegment(logicTableName, Collections.singletonList("ds_${0..1}.t_order${0..1}"), keyGenerator, null);
        result.setTableStrategySegment(new ShardingStrategySegment("standard", "product_id", new AlgorithmSegment("CORE.STANDARD.FIXTURE", new Properties())));
        AlgorithmSegment databaseAlgorithmSegment = new AlgorithmSegment("inline", PropertiesBuilder.build(new PropertiesBuilder.Property("algorithm-expression", "ds_${user_id % 2}")));
        result.setDatabaseStrategySegment(new ShardingStrategySegment("standard", "product_id", databaseAlgorithmSegment));
        return result;
    }
}
