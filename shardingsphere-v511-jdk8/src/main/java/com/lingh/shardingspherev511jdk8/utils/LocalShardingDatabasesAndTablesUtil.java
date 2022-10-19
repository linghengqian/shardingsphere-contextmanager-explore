package com.lingh.shardingspherev511jdk8.utils;

import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.sharding.algorithm.config.AlgorithmProvidedShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;

public class LocalShardingDatabasesAndTablesUtil {
    public static void updateActualDataNodesByVintage(ShardingSphereDataSource dataSource, String schemaName, String logicTableName, String newActualDataNodes) {
        ContextManager contextManager = dataSource.getContextManager();
        Collection<RuleConfiguration> newRuleConfigList = new LinkedList<>();
        Collection<RuleConfiguration> oldRuleConfigList = contextManager.getMetaDataContexts()
                .getMetaData(schemaName)
                .getRuleMetaData()
                .getConfigurations();
        oldRuleConfigList.stream()
                .filter(oldRuleConfig -> oldRuleConfig instanceof AlgorithmProvidedShardingRuleConfiguration)
                .map(oldRuleConfig -> (AlgorithmProvidedShardingRuleConfiguration) oldRuleConfig)
                .forEach(oldAlgorithmConfig -> {
                    AlgorithmProvidedShardingRuleConfiguration newAlgorithmConfig = new AlgorithmProvidedShardingRuleConfiguration();
                    Collection<ShardingTableRuleConfiguration> newTableRuleConfigList = new LinkedList<>();
                    Collection<ShardingTableRuleConfiguration> oldTableRuleConfigList = oldAlgorithmConfig.getTables();
                    oldTableRuleConfigList.forEach(oldTableRuleConfig -> {
                        if (logicTableName.equals(oldTableRuleConfig.getLogicTable())) {
                            ShardingTableRuleConfiguration newTableRuleConfig = new ShardingTableRuleConfiguration(oldTableRuleConfig.getLogicTable(), newActualDataNodes);
                            newTableRuleConfig.setTableShardingStrategy(oldTableRuleConfig.getTableShardingStrategy());
                            newTableRuleConfig.setDatabaseShardingStrategy(oldTableRuleConfig.getDatabaseShardingStrategy());
                            newTableRuleConfig.setKeyGenerateStrategy(oldTableRuleConfig.getKeyGenerateStrategy());
                            newTableRuleConfigList.add(newTableRuleConfig);
                        } else {
                            newTableRuleConfigList.add(oldTableRuleConfig);
                        }
                    });
                    newAlgorithmConfig.setTables(newTableRuleConfigList);
                    newAlgorithmConfig.setAutoTables(oldAlgorithmConfig.getAutoTables());
                    newAlgorithmConfig.setBindingTableGroups(oldAlgorithmConfig.getBindingTableGroups());
                    newAlgorithmConfig.setBroadcastTables(oldAlgorithmConfig.getBroadcastTables());
                    newAlgorithmConfig.setDefaultDatabaseShardingStrategy(oldAlgorithmConfig.getDefaultDatabaseShardingStrategy());
                    newAlgorithmConfig.setDefaultTableShardingStrategy(oldAlgorithmConfig.getDefaultTableShardingStrategy());
                    newAlgorithmConfig.setDefaultKeyGenerateStrategy(oldAlgorithmConfig.getDefaultKeyGenerateStrategy());
                    newAlgorithmConfig.setDefaultShardingColumn(oldAlgorithmConfig.getDefaultShardingColumn());
                    newAlgorithmConfig.setShardingAlgorithms(oldAlgorithmConfig.getShardingAlgorithms());
                    newAlgorithmConfig.setKeyGenerators(oldAlgorithmConfig.getKeyGenerators());
                    newRuleConfigList.add(newAlgorithmConfig);
                });
        contextManager.alterRuleConfiguration(schemaName, newRuleConfigList);
    }

    public static String getActualDataNodesByVintage(ShardingSphereDataSource dataSource, String schemaName, String logicTableName) {
        Collection<RuleConfiguration> oldRuleConfigList = dataSource.getContextManager()
                .getMetaDataContexts()
                .getMetaData(schemaName)
                .getRuleMetaData()
                .getConfigurations();
        AtomicReference<String> oldActualDataNodes = new AtomicReference<>();
        oldRuleConfigList.stream()
                .filter(oldRuleConfig -> oldRuleConfig instanceof AlgorithmProvidedShardingRuleConfiguration)
                .map(oldRuleConfig -> (AlgorithmProvidedShardingRuleConfiguration) oldRuleConfig)
                .forEach(oldAlgorithmConfig -> oldAlgorithmConfig.getTables()
                        .stream()
                        .filter(oldTableRuleConfig -> logicTableName.equals(oldTableRuleConfig.getLogicTable()))
                        .findFirst()
                        .ifPresent(oldTableRuleConfig -> oldActualDataNodes.set(oldTableRuleConfig.getActualDataNodes())));
        return oldActualDataNodes.get();
    }
}
