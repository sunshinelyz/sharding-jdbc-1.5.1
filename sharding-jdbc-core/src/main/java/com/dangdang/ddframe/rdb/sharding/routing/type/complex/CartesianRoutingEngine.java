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

package com.dangdang.ddframe.rdb.sharding.routing.type.complex;

import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingEngine;
import com.dangdang.ddframe.rdb.sharding.routing.type.RoutingResult;
import com.dangdang.ddframe.rdb.sharding.routing.type.TableUnit;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 笛卡尔积的库表路由.
 * 
 * @author zhangliang
 */
@RequiredArgsConstructor
@Slf4j
public final class CartesianRoutingEngine implements RoutingEngine {

//    路由结果
    private final Collection<RoutingResult> routingResults;
    
    @Override
    public CartesianRoutingResult route() {
//        创建笛卡尔积路由对象
        CartesianRoutingResult result = new CartesianRoutingResult();
//        遍历数据源和逻辑表集合的映射关系
        for (Entry<String, Set<String>> entry : getDataSourceLogicTablesMap().entrySet()) {
//            获取物理表集合
            List<Set<String>> actualTableGroups = getActualTableGroups(entry.getKey(), entry.getValue());
//            根据数据源和物理表集合获取具体的执行单元
            List<Set<TableUnit>> tableUnitGroups = toTableUnitGroups(entry.getKey(), actualTableGroups);
//            组装笛卡尔积路由结果集对象
            result.merge(entry.getKey(), getCartesianTableReferences(Sets.cartesianProduct(tableUnitGroups)));
        }
        log.trace("cartesian tables sharding result: {}", result);
        return result;
    }
    
    private Map<String, Set<String>> getDataSourceLogicTablesMap() {
//        获得数据源名称
        Collection<String> intersectionDataSources = getIntersectionDataSources();
        Map<String, Set<String>> result = new HashMap<>(routingResults.size());
        for (RoutingResult each : routingResults) {
//            遍历数据源名称和逻辑表结合之间的映射关系
            for (Entry<String, Set<String>> entry : each.getTableUnits().getDataSourceLogicTablesMap(intersectionDataSources).entrySet()) {
                if (result.containsKey(entry.getKey())) {
                    result.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }
    
    private Collection<String> getIntersectionDataSources() {
        Collection<String> result = new HashSet<>();
        for (RoutingResult each : routingResults) {
            if (result.isEmpty()) {
                result.addAll(each.getTableUnits().getDataSourceNames());
            }
            result.retainAll(each.getTableUnits().getDataSourceNames());
        }
        return result;
    }
    
    private List<Set<String>> getActualTableGroups(final String dataSource, final Set<String> logicTables) {
        List<Set<String>> result = new ArrayList<>(logicTables.size());
        for (RoutingResult each : routingResults) {
//            根据数据源、逻辑表回去物理表集合
            result.addAll(each.getTableUnits().getActualTableNameGroups(dataSource, logicTables));
        }
        return result;
    }
    
    private List<Set<TableUnit>> toTableUnitGroups(final String dataSource, final List<Set<String>> actualTableGroups) {
        List<Set<TableUnit>> result = new ArrayList<>(actualTableGroups.size());
        for (Set<String> each : actualTableGroups) {
            result.add(new HashSet<>(Lists.transform(new ArrayList<>(each), new Function<String, TableUnit>() {
    
                @Override
                public TableUnit apply(final String input) {
                    return findTableUnit(dataSource, input);
                }
            })));
        }
        return result;
    }
    
    private TableUnit findTableUnit(final String dataSource, final String actualTable) {
        for (RoutingResult each : routingResults) {
            Optional<TableUnit> result = each.getTableUnits().findTableUnit(dataSource, actualTable);
            if (result.isPresent()) {
                return result.get();
            }
        }
        throw new IllegalStateException(String.format("Cannot found routing table factor, data source: %s, actual table: %s", dataSource, actualTable));
    }
    
    private List<CartesianTableReference> getCartesianTableReferences(final Set<List<TableUnit>> cartesianTableUnitGroups) {
        List<CartesianTableReference> result = new ArrayList<>(cartesianTableUnitGroups.size());
        for (List<TableUnit> each : cartesianTableUnitGroups) {
            result.add(new CartesianTableReference(each));
        }
        return result;
    }
}
