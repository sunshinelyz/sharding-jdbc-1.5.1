/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.sharding.config.common.api;

import com.dangdang.ddframe.rdb.sharding.api.rule.BindingTableRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.MultipleKeysDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.SingleKeyDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.MultipleKeysTableShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.SingleKeyTableShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.BindingTableRuleConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.GenerateKeyColumnConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.ShardingRuleConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.StrategyConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.TableRuleConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.internal.algorithm.ClosureDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.config.common.internal.algorithm.ClosureTableShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.config.common.internal.parser.InlineParser;
import com.dangdang.ddframe.rdb.sharding.keygen.KeyGenerator;
import com.dangdang.ddframe.rdb.sharding.routing.strategy.MultipleKeysShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.routing.strategy.ShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.routing.strategy.ShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.routing.strategy.SingleKeyShardingAlgorithm;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 分片规则构建器.
 * 
 * @author gaohongtao
 */
public final class ShardingRuleBuilder {
    
    private final String logRoot;
    
    private final Map<String, DataSource> externalDataSourceMap;
    
    private final ShardingRuleConfig shardingRuleConfig;
    
    public ShardingRuleBuilder(final ShardingRuleConfig shardingRuleConfig) {
        this("default", shardingRuleConfig);
    }
    
    public ShardingRuleBuilder(final String logRoot, final ShardingRuleConfig shardingRuleConfig) {
        this(logRoot, Collections.<String, DataSource>emptyMap(), shardingRuleConfig);
    }
    
    public ShardingRuleBuilder(final String logRoot, final Map<String, DataSource> externalDataSourceMap, final ShardingRuleConfig shardingRuleConfig) {
        this.logRoot = logRoot;
        this.externalDataSourceMap = (null ==  externalDataSourceMap) ? Collections.<String, DataSource>emptyMap() : externalDataSourceMap;
        this.shardingRuleConfig = shardingRuleConfig;
    }
    
    /**
     * 构建分片规则.
     * 
     * @return 分片规则对象
     */
    public ShardingRule build() {
//        构建数据库分片规则
        DataSourceRule dataSourceRule = buildDataSourceRule();
//        构建表规则配置
        Collection<TableRule> tableRules = buildTableRules(dataSourceRule);
//        创建分片配置对象构造器
        com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule.ShardingRuleBuilder shardingRuleBuilder = ShardingRule.builder().dataSourceRule(dataSourceRule);
        if (!Strings.isNullOrEmpty(shardingRuleConfig.getKeyGeneratorClass())) {//指定了自动创建分布式id的实现类
//            创建分布式id
            shardingRuleBuilder.keyGenerator(loadClass(shardingRuleConfig.getKeyGeneratorClass(), KeyGenerator.class));
        }
//        装载分片规则配置构造器的一些参数
        return shardingRuleBuilder.tableRules(tableRules).bindingTableRules(buildBindingTableRules(tableRules))
                .databaseShardingStrategy(buildShardingStrategy(shardingRuleConfig.getDefaultDatabaseStrategy(), DatabaseShardingStrategy.class))
                .tableShardingStrategy(buildShardingStrategy(shardingRuleConfig.getDefaultTableStrategy(), TableShardingStrategy.class)).build();
    }
    
    private DataSourceRule buildDataSourceRule() {
//        判断解析过的数据库对象是不是为空
        Preconditions.checkArgument(!shardingRuleConfig.getDataSource().isEmpty() || !externalDataSourceMap.isEmpty(), "Sharding JDBC: No data source config");
//        如果解析过数据库对象为空，就获取默认的数据库名组成数据库对象map
        return shardingRuleConfig.getDataSource().isEmpty() ? new DataSourceRule(externalDataSourceMap, shardingRuleConfig.getDefaultDataSourceName())
                : new DataSourceRule(shardingRuleConfig.getDataSource(), shardingRuleConfig.getDefaultDataSourceName());
    }
    
    private Collection<TableRule> buildTableRules(final DataSourceRule dataSourceRule) {
//        获取表规则配置集合
        Collection<TableRule> result = new ArrayList<>(shardingRuleConfig.getTables().size());
        for (Entry<String, TableRuleConfig> each : shardingRuleConfig.getTables().entrySet()) {
            String logicTable = each.getKey();//这里的key存储的是逻辑表，value是表规则配置对象
            TableRuleConfig tableRuleConfig = each.getValue();
            //获取表规则配置构造器对象，这里是链式调用的用法，使代码更简洁
            TableRule.TableRuleBuilder tableRuleBuilder = TableRule.builder(logicTable).dataSourceRule(dataSourceRule)
                    .dynamic(tableRuleConfig.isDynamic())//是否动态分表
//                    构造数据库分片策略并加入到表规则构造器对象
                    .databaseShardingStrategy(buildShardingStrategy(tableRuleConfig.getDatabaseStrategy(), DatabaseShardingStrategy.class))
//                    构造表分片策略并加入到表规则构造器对象
                    .tableShardingStrategy(buildShardingStrategy(tableRuleConfig.getTableStrategy(), TableShardingStrategy.class));
            if (null != tableRuleConfig.getActualTables()) {//物理表不为空
//                解析具体的物理表对表规则构造器进行装载
                tableRuleBuilder.actualTables(new InlineParser(tableRuleConfig.getActualTables()).evaluate());
            }
//            解析配置的数据库名对表规则构造器进行装载
            if (!Strings.isNullOrEmpty(tableRuleConfig.getDataSourceNames())) {
                tableRuleBuilder.dataSourceNames(new InlineParser(tableRuleConfig.getDataSourceNames()).evaluate());
            }
//            分布式id解析
            buildGenerateKeyColumn(tableRuleBuilder, tableRuleConfig);
            result.add(tableRuleBuilder.build());
        }
        return result;
    }
    
    private void buildGenerateKeyColumn(final TableRule.TableRuleBuilder tableRuleBuilder, final TableRuleConfig tableRuleConfig) {
        for (GenerateKeyColumnConfig each : tableRuleConfig.getGenerateKeyColumns()) {
            if (Strings.isNullOrEmpty(each.getColumnKeyGeneratorClass())) {
                tableRuleBuilder.generateKeyColumn(each.getColumnName());
            } else {
                tableRuleBuilder.generateKeyColumn(each.getColumnName(), loadClass(each.getColumnKeyGeneratorClass(), KeyGenerator.class));
            }
        }
    }

//    构造绑定表规则
    private Collection<BindingTableRule> buildBindingTableRules(final Collection<TableRule> tableRules) {
        Collection<BindingTableRule> result = new ArrayList<>(shardingRuleConfig.getBindingTables().size());
        for (BindingTableRuleConfig each : shardingRuleConfig.getBindingTables()) {
            result.add(new BindingTableRule(Lists.transform(new InlineParser(each.getTableNames()).split(), new Function<String, TableRule>() {
                
                @Override
                public TableRule apply(final String input) {
                    return findTableRuleByLogicTableName(tableRules, input);
                }
            })));
        }
        return result;
    }
    
    private TableRule findTableRuleByLogicTableName(final Collection<TableRule> tableRules, final String logicTableName) {
        for (TableRule each : tableRules) {
            if (logicTableName.equalsIgnoreCase(each.getLogicTable())) {
                return each;
            }
        }
        throw new IllegalArgumentException(String.format("Sharding JDBC: Binding table `%s` is not an available Table rule", logicTableName));
    }
    
    private <T extends ShardingStrategy> T buildShardingStrategy(final StrategyConfig config, final Class<T> returnClass) {
        if (null == config) {//必须配置了数据库分片，程序才能继续
            return null;
        }
//        这里面大量用到了guava这个类库，使代码更优雅，//这里配置的分库分表策略表达式和策略实现类只能实现一种，也必须配置一种
        Preconditions.checkArgument(Strings.isNullOrEmpty(config.getAlgorithmExpression()) && !Strings.isNullOrEmpty(config.getAlgorithmClassName())
                || !Strings.isNullOrEmpty(config.getAlgorithmExpression()) && Strings.isNullOrEmpty(config.getAlgorithmClassName()));
//        判断DatabaseShardingStrategy的Class对象是否和returnClass相同或者是其子类或者子接口，后面数据库策略类参数也是这样的判断
        Preconditions.checkState(returnClass.isAssignableFrom(DatabaseShardingStrategy.class) || returnClass.isAssignableFrom(TableShardingStrategy.class), "Sharding-JDBC: returnClass is illegal");
        List<String> shardingColumns = new InlineParser(config.getShardingColumns()).split();//这里的分库策略可以是根据多个字段
        if (Strings.isNullOrEmpty(config.getAlgorithmClassName())) {//如果这里没有指定分片策略类，就走inline表达式解析的逻辑
            return buildShardingAlgorithmExpression(shardingColumns, config.getAlgorithmExpression(), returnClass);
        }
//        指定了分片策略实现类就走这个逻辑
        return buildShardingAlgorithmClassName(shardingColumns, config.getAlgorithmClassName(), returnClass);
    }
    
    @SuppressWarnings("unchecked")
    private <T extends ShardingStrategy> T buildShardingAlgorithmExpression(final List<String> shardingColumns, final String algorithmExpression, final Class<T> returnClass) {
        //inline表达式解析逻辑在ClosureTableShardingAlgorithm这个类实现
        return returnClass.isAssignableFrom(DatabaseShardingStrategy.class) ? (T) new DatabaseShardingStrategy(shardingColumns, new ClosureDatabaseShardingAlgorithm(algorithmExpression, logRoot))
                : (T) new TableShardingStrategy(shardingColumns, new ClosureTableShardingAlgorithm(algorithmExpression, logRoot));
    }
    
    @SuppressWarnings("unchecked")
    private <T extends ShardingStrategy> T buildShardingAlgorithmClassName(final List<String> shardingColumns, final String algorithmClassName, final Class<T> returnClass) {
        ShardingAlgorithm shardingAlgorithm;
        try {
//            实例化分片策略类，这里是java多态的用法，可以使实现逻辑更灵活
            shardingAlgorithm = (ShardingAlgorithm) Class.forName(algorithmClassName).newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
            throw new IllegalArgumentException(ex);
        }
//        判断分片策略配置的是单分片值策略还是多分片值策略
        Preconditions.checkState(shardingAlgorithm instanceof SingleKeyShardingAlgorithm || shardingAlgorithm instanceof MultipleKeysShardingAlgorithm, "Sharding-JDBC: algorithmClassName is illegal");
//        如果是配置的是单分片值策略
        if (shardingAlgorithm instanceof SingleKeyShardingAlgorithm) {
//            单分片值策略，配置了多个分片值字段会报错
            Preconditions.checkArgument(1 == shardingColumns.size(), "Sharding-JDBC: SingleKeyShardingAlgorithm must have only ONE sharding column");
//            根据策略类型去创建数据库分片策略对象还是去创建表分片策略对象，整个分片策略实现逻辑都是用的java多态的特性，这样的设计很巧妙，大大提供了程序的扩展性、灵活性
            return returnClass.isAssignableFrom(DatabaseShardingStrategy.class) ? (T) new DatabaseShardingStrategy(shardingColumns.get(0), (SingleKeyDatabaseShardingAlgorithm<?>) shardingAlgorithm)
                    : (T) new TableShardingStrategy(shardingColumns.get(0), (SingleKeyTableShardingAlgorithm<?>) shardingAlgorithm);
        }
//        这里同样也是一样的逻辑去创建数据库分片对象和表分片对象
        return returnClass.isAssignableFrom(DatabaseShardingStrategy.class) ? (T) new DatabaseShardingStrategy(shardingColumns, (MultipleKeysDatabaseShardingAlgorithm) shardingAlgorithm) 
                : (T) new TableShardingStrategy(shardingColumns, (MultipleKeysTableShardingAlgorithm) shardingAlgorithm);
    }
    
    @SuppressWarnings("unchecked")
    private <T> Class<? extends T> loadClass(final String className, final Class<T> superClass) {
        try {
            return (Class<? extends T>) superClass.getClassLoader().loadClass(className);
        } catch (final ClassNotFoundException ex) {
            throw new IllegalArgumentException(ex);
        }
    }
}
