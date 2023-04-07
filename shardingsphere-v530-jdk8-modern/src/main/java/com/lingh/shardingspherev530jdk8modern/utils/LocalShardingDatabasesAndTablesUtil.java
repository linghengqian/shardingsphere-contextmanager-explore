package com.lingh.shardingspherev530jdk8modern.utils;

import org.apache.shardingsphere.distsql.parser.segment.AlgorithmSegment;
import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
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

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class LocalShardingDatabasesAndTablesUtil {
    public static String getActualDataNodesByVintage(DataSource dataSource, String databaseName, String logicTableName) {
        AtomicReference<String> currentActualDataNodes = new AtomicReference<>();
        Optional<ShardingRule> singleRule = getContextManager(dataSource).getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(databaseName)
                .getRuleMetaData()
                .findSingleRule(ShardingRule.class);
        assert singleRule.isPresent();
        ShardingRuleConfiguration currentRuleConfig = (ShardingRuleConfiguration) singleRule.get().getConfiguration();
        currentRuleConfig.getTables()
                .stream()
                .filter(table -> logicTableName.equals(table.getLogicTable()))
                .findFirst()
                .ifPresent(table -> currentActualDataNodes.set(table.getActualDataNodes()));
        return currentActualDataNodes.get();
    }

    public static void updateActualDataNodesByJupiter(DataSource dataSource, String databaseName, String logicTableName, String newActualDataNodes) {
        ContextManager contextManager = getContextManager(dataSource);
        ShardingSphereDatabase database = contextManager.getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(databaseName);
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
        result.setKeyGenerateStrategySegment(
                new KeyGenerateStrategySegment("product_id",
                new AlgorithmSegment("DISTSQL.FIXTURE", new Properties()))
        );
        result.setShardingColumn("order_id");
        result.setShardingAlgorithmSegment(new AlgorithmSegment("FOO.DISTSQL.FIXTURE", PropertiesBuilder.build(new PropertiesBuilder.Property("", ""))));
        return result;
    }

    private static TableRuleSegment createCompleteTableRule(final String logicTableName) {
        TableRuleSegment result = new TableRuleSegment(
                logicTableName,
                Collections.singletonList("ds_${0..1}.t_order${0..1}"),
                new KeyGenerateStrategySegment("product_id", new AlgorithmSegment("DISTSQL.FIXTURE", new Properties())),
                null
        );
        result.setTableStrategySegment(new ShardingStrategySegment("standard",
                "product_id", new AlgorithmSegment("CORE.STANDARD.FIXTURE", new Properties())));
        result.setDatabaseStrategySegment(
                new ShardingStrategySegment("standard", "product_id",
                        new AlgorithmSegment("inline",
                                PropertiesBuilder.build(new PropertiesBuilder.Property("algorithm-expression", "ds_${user_id % 2}"))))
        );
        return result;
    }


    private static ContextManager getContextManager(final DataSource dataSource) {
        try (ShardingSphereConnection connection = dataSource.getConnection().unwrap(ShardingSphereConnection.class)) {
            return connection.getContextManager();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
