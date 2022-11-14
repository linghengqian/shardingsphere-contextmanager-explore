package com.lingh.shardingspherev512jdk8.utils;

import lombok.SneakyThrows;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.sharding.algorithm.config.AlgorithmProvidedShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;

public class LocalShardingDatabasesAndTablesUtil {
    public static void updateActualDataNodesByVintage(ShardingSphereDataSource dataSource, String databaseName, String logicTableName, String newActualDataNodes) {
        ContextManager contextManager = getContextManager(dataSource);
        Collection<RuleConfiguration> newRuleConfigList = new LinkedList<>();
        Collection<RuleConfiguration> oldRuleConfigList = getContextManager(dataSource)
                .getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(databaseName)
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
                        if (oldTableRuleConfig.getLogicTable().equals(logicTableName)) {
                            ShardingTableRuleConfiguration newTableRuleConfig = new ShardingTableRuleConfiguration(oldTableRuleConfig.getLogicTable(), newActualDataNodes);
                            newTableRuleConfig.setReplaceTablePrefix(oldTableRuleConfig.getReplaceTablePrefix());
                            newTableRuleConfig.setDatabaseShardingStrategy(oldTableRuleConfig.getDatabaseShardingStrategy());
                            newTableRuleConfig.setTableShardingStrategy(oldTableRuleConfig.getTableShardingStrategy());
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
        contextManager.alterRuleConfiguration(databaseName, newRuleConfigList);
    }

    public static String getActualDataNodesByVintage(ShardingSphereDataSource dataSource, String databaseName, String logicTableName) {
        Collection<RuleConfiguration> oldRuleConfigList = getContextManager(dataSource)
                .getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(databaseName)
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

    public static void updateActualDataNodesByJupiter(ShardingSphereDataSource dataSource, String databaseName, String logicTableName, String newActualDataNodes) {
        ContextManager contextManager = getContextManager(dataSource);
        Collection<RuleConfiguration> newRuleConfigList = new LinkedList<>();
        Collection<RuleConfiguration> oldRuleConfigList = contextManager
                .getMetaDataContexts()
                .getMetaData()
                .getDatabases()
                .get(databaseName)
                .getRuleMetaData()
                .getConfigurations();
        oldRuleConfigList.stream()
                .filter(oldRuleConfig -> oldRuleConfig instanceof AlgorithmProvidedShardingRuleConfiguration)
                .map(oldRuleConfig -> (AlgorithmProvidedShardingRuleConfiguration) oldRuleConfig)
                .forEach(oldAlgorithmConfig -> {
                    Collection<ShardingTableRuleConfiguration> newTableRuleConfigList = new LinkedList<>();
                    oldAlgorithmConfig.getTables().forEach(oldTableRuleConfig -> {
                        if (logicTableName.equals(oldTableRuleConfig.getLogicTable())) {
                            ShardingTableRuleConfiguration newTableRuleConfig = new ShardingTableRuleConfiguration(logicTableName, newActualDataNodes);
                            newTableRuleConfig.setReplaceTablePrefix(oldTableRuleConfig.getReplaceTablePrefix());
                            newTableRuleConfig.setDatabaseShardingStrategy(oldTableRuleConfig.getDatabaseShardingStrategy());
                            newTableRuleConfig.setTableShardingStrategy(oldTableRuleConfig.getTableShardingStrategy());
                            newTableRuleConfig.setKeyGenerateStrategy(oldTableRuleConfig.getKeyGenerateStrategy());
                            newTableRuleConfigList.add(newTableRuleConfig);
                        } else {
                            newTableRuleConfigList.add(oldTableRuleConfig);
                        }
                    });
                    oldAlgorithmConfig.setTables(newTableRuleConfigList);
                    newRuleConfigList.add(oldAlgorithmConfig);
                });
        contextManager.alterRuleConfiguration(databaseName, newRuleConfigList);
        contextManager.reloadMetaData(databaseName);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    private static ContextManager getContextManager(final ShardingSphereDataSource dataSource) {
        Field field = ShardingSphereDataSource.class.getDeclaredField("contextManager");
        field.setAccessible(true);
        return (ContextManager) field.get(dataSource);
    }
}
