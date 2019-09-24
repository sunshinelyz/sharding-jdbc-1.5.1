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

package com.dangdang.ddframe.rdb.sharding.routing.strategy;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;

/**
 * 分片策略.
 * 
 * @author zhangliang
 */
public class ShardingStrategy {

//    分片列
    @Getter
    private final Collection<String> shardingColumns;

//    分片算法
    private final ShardingAlgorithm shardingAlgorithm;
    
    public ShardingStrategy(final String shardingColumn, final ShardingAlgorithm shardingAlgorithm) {
        this(Collections.singletonList(shardingColumn), shardingAlgorithm);
    }
    
    public ShardingStrategy(final Collection<String> shardingColumns, final ShardingAlgorithm shardingAlgorithm) {
        this.shardingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        this.shardingColumns.addAll(shardingColumns);
        this.shardingAlgorithm = shardingAlgorithm;
    }
    
    /**
     * 计算静态分片.
     *
     * @param availableTargetNames 所有的可用分片资源集合
     * @param shardingValues 分片值集合
     * @return 分库后指向的数据源名称集合
     */
    public Collection<String> doStaticSharding(final Collection<String> availableTargetNames, final Collection<ShardingValue<?>> shardingValues) {
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
//        如果没有解析到传入的数据源分片值，要走全库路由
        if (shardingValues.isEmpty()) {
            result.addAll(availableTargetNames);
        } else {
//            如果传入分片值，根据分片值去获取具体的数据源
            result.addAll(doSharding(shardingValues, availableTargetNames));
        }
        return result;
    }
    
    /**
     * 计算动态分片.
     *
     * @param shardingValues 分片值集合
     * @return 分库后指向的分片资源集合
     */
    public Collection<String> doDynamicSharding(final Collection<ShardingValue<?>> shardingValues) {//doDynamicSharding
        Preconditions.checkState(!shardingValues.isEmpty(), "Dynamic table should contain sharding value.");
        Collection<String> availableTargetNames = Collections.emptyList();
        Collection<String> result = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        result.addAll(doSharding(shardingValues, availableTargetNames));
        return result;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Collection<String> doSharding(final Collection<ShardingValue<?>> shardingValues, final Collection<String> availableTargetNames) {
//        如果没分片
        if (shardingAlgorithm instanceof NoneKeyShardingAlgorithm) {
            return Collections.singletonList(((NoneKeyShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues.iterator().next()));
        }
//        如果按一个分片值分片
        if (shardingAlgorithm instanceof SingleKeyShardingAlgorithm) {
            SingleKeyShardingAlgorithm<?> singleKeyShardingAlgorithm = (SingleKeyShardingAlgorithm<?>) shardingAlgorithm;
            ShardingValue shardingValue = shardingValues.iterator().next();
            switch (shardingValue.getType()) {
                case SINGLE:
//                    = 元算符分片
                    return Collections.singletonList(singleKeyShardingAlgorithm.doEqualSharding(availableTargetNames, shardingValue));
                case LIST:
//                    in运算符分片
                    return singleKeyShardingAlgorithm.doInSharding(availableTargetNames, shardingValue);
                case RANGE:
//                    between运算符分片
                    return singleKeyShardingAlgorithm.doBetweenSharding(availableTargetNames, shardingValue);
                default:
//                    现在只支持这三种运算符分片
                    throw new UnsupportedOperationException(shardingValue.getType().getClass().getName());
            }
        }
//        如果是多个分片值
        if (shardingAlgorithm instanceof MultipleKeysShardingAlgorithm) {
            return ((MultipleKeysShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues);
        }
//        其他方式的分片不支持
        throw new UnsupportedOperationException(shardingAlgorithm.getClass().getName());
    }
}
