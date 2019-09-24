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

package com.dangdang.ddframe.rdb.sharding.routing.router;

import com.codahale.metrics.Timer.Context;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.constant.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.jdbc.core.ShardingContext;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;
import com.dangdang.ddframe.rdb.sharding.parsing.SQLParsingEngine;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.GeneratedKey;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatement;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dml.insert.InsertStatement;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.dql.select.SelectStatement;
import com.dangdang.ddframe.rdb.sharding.rewrite.SQLBuilder;
import com.dangdang.ddframe.rdb.sharding.rewrite.SQLRewriteEngine;
import com.dangdang.ddframe.rdb.sharding.routing.SQLExecutionUnit;
import com.dangdang.ddframe.rdb.sharding.routing.SQLRouteResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.CartesianDataSource;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.CartesianRoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.CartesianTableReference;
import com.dangdang.ddframe.rdb.sharding.routing.type.complex.ComplexRoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.simple.SimpleRoutingEngine;
import com.dangdang.ddframe.rdb.sharding.util.SQLLogger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 需要解析的SQL路由器.
 * 
 * @author zhangiang
 */
public final class ParsingSQLRouter implements SQLRouter {

//    分库分表配置对象
    private final ShardingRule shardingRule;

//    支持的数据库类型
    private final DatabaseType databaseType;

//    是否要展示sql
    private final boolean showSQL;
    
    private final List<Number> generatedKeys;

//    上面这些属性值都是存储在分片上下文中
    public ParsingSQLRouter(final ShardingContext shardingContext) {
        shardingRule = shardingContext.getShardingRule();
        databaseType = shardingContext.getDatabaseType();
        showSQL = shardingContext.isShowSQL();
        generatedKeys = new LinkedList<>();
    }
    
    @Override
    public SQLStatement parse(final String logicSQL, final int parametersSize) {
//        创建sql解析引擎
        SQLParsingEngine parsingEngine = new SQLParsingEngine(databaseType, logicSQL, shardingRule);
//        开启度量上下文
        Context context = MetricsContext.start("Parse SQL");

//        sql解析器解析获得sql语句对象
        SQLStatement result = parsingEngine.parse();
        if (result instanceof InsertStatement) {
            ((InsertStatement) result).appendGenerateKeyToken(shardingRule, parametersSize);
        }
        MetricsContext.stop(context);
        return result;
    }
    
    @Override
    public SQLRouteResult route(final String logicSQL, final List<Object> parameters, final SQLStatement sqlStatement) {
        final Context context = MetricsContext.start("Route SQL");
        SQLRouteResult result = new SQLRouteResult(sqlStatement);
//        如果是insert语句去生成分布式逐渐的逻辑
        if (sqlStatement instanceof InsertStatement && null != ((InsertStatement) sqlStatement).getGeneratedKey()) {
            processGeneratedKey(parameters, (InsertStatement) sqlStatement, result);
        }
//        进行sql路由返回路由结果
        RoutingResult routingResult = route(parameters, sqlStatement);
//        sql改写
        SQLRewriteEngine rewriteEngine = new SQLRewriteEngine(shardingRule, logicSQL, sqlStatement);
//        判断是否为单库表路由
        boolean isSingleRouting = routingResult.isSingleRouting();
//        获取select语句的分页对象
        if (sqlStatement instanceof SelectStatement && null != ((SelectStatement) sqlStatement).getLimit()) {
//            处理分页
            processLimit(parameters, (SelectStatement) sqlStatement, isSingleRouting);
        }
//        不是单库表路由，slq改写引擎返回一个sql构造器
        SQLBuilder sqlBuilder = rewriteEngine.rewrite(!isSingleRouting);
//        如果路由结果集是笛卡尔积结果集
        if (routingResult instanceof CartesianRoutingResult) {
//            遍历数据源
            for (CartesianDataSource cartesianDataSource : ((CartesianRoutingResult) routingResult).getRoutingDataSources()) {
//               遍历笛卡尔积表路由组
                for (CartesianTableReference cartesianTableReference : cartesianDataSource.getRoutingTableReferences()) {
//                    拼装最小执行单元，并装载路由结果集对象
                    result.getExecutionUnits().add(new SQLExecutionUnit(cartesianDataSource.getDataSource(), rewriteEngine.generateSQL(cartesianTableReference, sqlBuilder)));
                }
            }
        } else {
//            简单路由拼装最小执行单元
            for (TableUnit each : routingResult.getTableUnits().getTableUnits()) {
                result.getExecutionUnits().add(new SQLExecutionUnit(each.getDataSourceName(), rewriteEngine.generateSQL(each, sqlBuilder)));
            }
        }
        MetricsContext.stop(context);
        if (showSQL) {
            SQLLogger.logSQL(logicSQL, sqlStatement, result.getExecutionUnits(), parameters);
        }
        return result;
    }
    
    private RoutingResult route(final List<Object> parameters, final SQLStatement sqlStatement) {
        Collection<String> tableNames = sqlStatement.getTables().getTableNames();
        RoutingEngine routingEngine;
//        如果表集合是1，或者是绑定表路由就走简单路由规则
        if (1 == tableNames.size() || shardingRule.isAllBindingTables(tableNames)) {
            routingEngine = new SimpleRoutingEngine(shardingRule, parameters, tableNames.iterator().next(), sqlStatement);//单表路由
        } else {
            // TODO 可配置是否执行笛卡尔积
            routingEngine = new ComplexRoutingEngine(shardingRule, parameters, tableNames, sqlStatement);
        }
        return routingEngine.route();//tianhe TODO 笛卡尔积
    }
    
    private void processGeneratedKey(final List<Object> parameters, final InsertStatement insertStatement, final SQLRouteResult sqlRouteResult) {
        GeneratedKey generatedKey = insertStatement.getGeneratedKey();
        if (parameters.isEmpty()) {
            sqlRouteResult.getGeneratedKeys().add(generatedKey.getValue());
        } else if (parameters.size() == generatedKey.getIndex()) {
            Number key = shardingRule.generateKey(insertStatement.getTables().getSingleTableName());
            parameters.add(key);
            setGeneratedKeys(sqlRouteResult, key);
        } else if (-1 != generatedKey.getIndex()) {
            setGeneratedKeys(sqlRouteResult, (Number) parameters.get(generatedKey.getIndex()));
        }
    }
    
    private void setGeneratedKeys(final SQLRouteResult sqlRouteResult, final Number generatedKey) {
        generatedKeys.add(generatedKey);
        sqlRouteResult.getGeneratedKeys().clear();
        sqlRouteResult.getGeneratedKeys().addAll(generatedKeys);
    }
    
    private void processLimit(final List<Object> parameters, final SelectStatement selectStatement, final boolean isSingleRouting) {
//        select语句排序项不为空    或者聚合选择项不为空  或者排序项和分组项不一致
        boolean isNeedFetchAll = (!selectStatement.getGroupByItems().isEmpty() || !selectStatement.getAggregationSelectItems().isEmpty()) && !selectStatement.isSameGroupByAndOrderByItems();
//        填充改写分页参数,!isSingleRouting注意这里只要不是单库表操作分页sql都会进行sql改写，改写成这样，改写前0，10，改写后0，10，改写前10，20 改写后0，20
        selectStatement.getLimit().processParameters(parameters, !isSingleRouting, isNeedFetchAll);
    }
}
