package com.lingh.shardingspherev521jdk8.utils;

import org.apache.shardingsphere.driver.jdbc.core.connection.ShardingSphereConnection;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.metadata.database.rule.ShardingSphereRuleMetaData;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.rule.ShardingRule;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;
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
        ShardingSphereRuleMetaData ruleMetaData = contextManager.getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(databaseName)
                .getRuleMetaData();
        Optional<ShardingRule> singleRule = ruleMetaData.findSingleRule(ShardingRule.class);
        assert singleRule.isPresent();
        ShardingRuleConfiguration currentRuleConfig = (ShardingRuleConfiguration) singleRule.get().getConfiguration();
        Collection<ShardingTableRuleConfiguration> toBeAlteredTableRuleConfigList = new LinkedList<>();
        currentRuleConfig.getTables().forEach(oldTableRuleConfig -> {
            if (logicTableName.equals(oldTableRuleConfig.getLogicTable())) {
                ShardingTableRuleConfiguration newTableRuleConfig = new ShardingTableRuleConfiguration(logicTableName, newActualDataNodes);
                newTableRuleConfig.setDatabaseShardingStrategy(oldTableRuleConfig.getDatabaseShardingStrategy());
                newTableRuleConfig.setTableShardingStrategy(oldTableRuleConfig.getTableShardingStrategy());
                newTableRuleConfig.setKeyGenerateStrategy(oldTableRuleConfig.getKeyGenerateStrategy());
                newTableRuleConfig.setAuditStrategy(oldTableRuleConfig.getAuditStrategy());
                toBeAlteredTableRuleConfigList.add(newTableRuleConfig);
            } else {
                toBeAlteredTableRuleConfigList.add(oldTableRuleConfig);
            }
        });
        currentRuleConfig.setTables(toBeAlteredTableRuleConfigList);
        Collection<RuleConfiguration> toBeAlteredRuleConfigList = new LinkedList<>();
        toBeAlteredRuleConfigList.add(currentRuleConfig);
        ruleMetaData.getRules().forEach(shardingSphereRule -> {
            if (!(shardingSphereRule instanceof ShardingRule)) {
                toBeAlteredRuleConfigList.add(shardingSphereRule.getConfiguration());
            }
        });
        contextManager.alterRuleConfiguration(databaseName, toBeAlteredRuleConfigList);
        contextManager.reloadDatabase(databaseName);
    }

    private static ContextManager getContextManager(final DataSource dataSource) {
        try (ShardingSphereConnection connection = dataSource.getConnection().unwrap(ShardingSphereConnection.class)) {
            return connection.getContextManager();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
