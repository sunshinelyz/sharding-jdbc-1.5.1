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

package com.dangdang.ddframe.rdb.sharding.spring.namespace.parser;

import com.dangdang.ddframe.rdb.sharding.config.common.api.config.GenerateKeyColumnConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.BindingTableRuleConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.ShardingRuleConfig;
import com.dangdang.ddframe.rdb.sharding.config.common.api.config.TableRuleConfig;
import com.dangdang.ddframe.rdb.sharding.spring.datasource.SpringShardingDataSource;
import com.dangdang.ddframe.rdb.sharding.spring.namespace.constants.ShardingJdbcDataSourceBeanDefinitionParserTag;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 基于Spring命名空间的数据源解析器.
 * 
 * @author caohao
 */
public class ShardingJdbcDataSourceBeanDefinitionParser extends AbstractBeanDefinitionParser {
    
    @Override
    //CHECKSTYLE:OFF
    protected AbstractBeanDefinition parseInternal(final Element element, final ParserContext parserContext) {
    //CHECKSTYLE:ON
        BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(SpringShardingDataSource.class);//初始化调用SpringShardingDataSource
        //有参构造参数，但是参数默认是null
        factory.addConstructorArgValue(parseShardingRuleConfig(element, parserContext));//设置构造参数的值
        //解析props节点，这个节点值是对sharding-jdbc的一些属性解析，比如允许不允许打印sharding-jdbc的日志
        factory.addConstructorArgValue(parseProperties(element, parserContext));
        factory.setDestroyMethodName("close");
        return factory.getBeanDefinition();
    }
    
    private BeanDefinition parseShardingRuleConfig(final Element element, final ParserContext parserContext) {
        //获取到sharding-rule这个节点的孩子节点
        Element shardingRuleElement = DomUtils.getChildElementByTagName(element, ShardingJdbcDataSourceBeanDefinitionParserTag.SHARDING_RULE_CONFIG_TAG);
        //调用ShardingRuleConfig的无参构造器初始化对象
        BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(ShardingRuleConfig.class);
        //给ShardingRuleConfig对象的dataSource属性赋值
        factory.addPropertyValue("dataSource", parseDataSources(shardingRuleElement, parserContext));
        //解析default-data-source这个标签的值并对ShardingRuleConfig对象的defaultDataSourceName属性赋值
        parseDefaultDataSource(factory, shardingRuleElement);
        //解析table-rules标签并对ShardingRuleConfig对象的tables属性赋值
        factory.addPropertyValue("tables", parseTableRulesConfig(shardingRuleElement));
        //解析binding-table-rules属性对ShardingRuleConfig对象的bindingTables属性赋值
        factory.addPropertyValue("bindingTables", parseBindingTablesConfig(shardingRuleElement));
//        解析default-database-strategy节点并对ShardingRuleConfig对象的defaultDatabaseStrategy属性赋值
        factory.addPropertyValue("defaultDatabaseStrategy", parseDefaultDatabaseStrategyConfig(shardingRuleElement));
//        解析default-table-strategy节点并对ShardingRuleConfig对象的defaultTableStrategy属性赋值
        factory.addPropertyValue("defaultTableStrategy", parseDefaultTableStrategyConfig(shardingRuleElement));
//        解析key-generator-class节点并对keyGeneratorClass属性赋值
        parseKeyGenerator(factory, shardingRuleElement);
        return factory.getBeanDefinition();
    }
    
    private void parseKeyGenerator(final BeanDefinitionBuilder factory, final Element element) {
        String keyGeneratorClass = element.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.KEY_GENERATOR_CLASS);
        if (!Strings.isNullOrEmpty(keyGeneratorClass)) {
            factory.addPropertyValue("keyGeneratorClass", keyGeneratorClass);
        }
    }
    
    private Map<String, BeanDefinition> parseDataSources(final Element element, final ParserContext parserContext) {
        //根据传递过来的节点解析这个data-sources标签，然后根据,进行转成成list，由此可见这个节点的值可以是多个数据源的string类型的字符串用，分开的
        List<String> dataSources = Splitter.on(",").trimResults().splitToList(element.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.DATA_SOURCES_TAG));
        Map<String, BeanDefinition> result = new ManagedMap<>(dataSources.size());
        for (String each : dataSources) {
            result.put(each, parserContext.getRegistry().getBeanDefinition(each));
        }
        return result;
    }
    
    private void parseDefaultDataSource(final BeanDefinitionBuilder factory, final Element element) {
        String defaultDataSource = element.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.DEFAULT_DATA_SOURCE_TAG);
        if (!Strings.isNullOrEmpty(defaultDataSource)) {
            factory.addPropertyValue("defaultDataSourceName", defaultDataSource);
        }
    }
    
    private Map<String, BeanDefinition> parseTableRulesConfig(final Element element) {
        //获取到这个节点对象table-rules
        Element tableRulesElement = DomUtils.getChildElementByTagName(element, ShardingJdbcDataSourceBeanDefinitionParserTag.TABLE_RULES_TAG);
        //获取到这个table-rule节点的值
        List<Element> tableRuleElements = DomUtils.getChildElementsByTagName(tableRulesElement, ShardingJdbcDataSourceBeanDefinitionParserTag.TABLE_RULE_TAG);
        Map<String, BeanDefinition> result = new ManagedMap<>(tableRuleElements.size());
        for (Element each : tableRuleElements) {
            //解析下级节点logic-table属性值作为map的key，解析其他子节点返回一个BeanDefinition对象作为map的value
            result.put(each.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.LOGIC_TABLE_ATTRIBUTE), parseTableRuleConfig(each));
        }
        return result;
    }
    
    private BeanDefinition parseTableRuleConfig(final Element tableElement) {
        BeanDefinitionBuilder factory = BeanDefinitionBuilder.rootBeanDefinition(TableRuleConfig.class);
        String dynamic = tableElement.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.DYNAMIC_TABLE_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(dynamic)) {
            factory.addPropertyValue("dynamic", dynamic);
        }
        String actualTables = tableElement.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.ACTUAL_TABLES_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(actualTables)) {
            factory.addPropertyValue("actualTables", actualTables);
        }
        String dataSourceNames = tableElement.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.DATA_SOURCE_NAMES_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(dataSourceNames)) {
            factory.addPropertyValue("dataSourceNames", dataSourceNames);
        }
        String databaseStrategy = tableElement.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.DATABASE_STRATEGY_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(databaseStrategy)) {
            factory.addPropertyReference("databaseStrategy", databaseStrategy);    
        }
        String tableStrategy = tableElement.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.TABLE_STRATEGY_ATTRIBUTE);
        if (!Strings.isNullOrEmpty(tableStrategy)) {
            factory.addPropertyReference("tableStrategy", tableStrategy);
        }
        List<Element> generateKeyColumns = DomUtils.getChildElementsByTagName(tableElement, ShardingJdbcDataSourceBeanDefinitionParserTag.GENERATE_KEY_COLUMN);
        if (null == generateKeyColumns || generateKeyColumns.isEmpty()) {
            return factory.getBeanDefinition();
        }
        factory.addPropertyValue("generateKeyColumns", Lists.transform(generateKeyColumns, new Function<Element, GenerateKeyColumnConfig>() {
            @Override
            public GenerateKeyColumnConfig apply(final Element input) {
                GenerateKeyColumnConfig result = new GenerateKeyColumnConfig();
                result.setColumnName(input.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.COLUMN_NAME));
                result.setColumnKeyGeneratorClass(input.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.COLUMN_KEY_GENERATOR_CLASS));
                return result;
            }
        }));
        return factory.getBeanDefinition();
    }
    
    private List<BeanDefinition> parseBindingTablesConfig(final Element element) {
//        获取到这个binding-table-rules节点对象
        Element bindingTableRulesElement = DomUtils.getChildElementByTagName(element, ShardingJdbcDataSourceBeanDefinitionParserTag.BINDING_TABLE_RULES_TAG);
        if (null == bindingTableRulesElement) {
            return Collections.emptyList();
        }
//        获取子节点binding-table-rule的节点对象
        List<Element> bindingTableRuleElements = DomUtils.getChildElementsByTagName(bindingTableRulesElement, ShardingJdbcDataSourceBeanDefinitionParserTag.BINDING_TABLE_RULE_TAG);
//        初始化BindingTableRuleConfig对象
        BeanDefinitionBuilder bindingTableRuleFactory = BeanDefinitionBuilder.rootBeanDefinition(BindingTableRuleConfig.class);
        List<BeanDefinition> result = new ManagedList<>(bindingTableRuleElements.size());
        for (Element bindingTableRuleElement : bindingTableRuleElements) {
//            解析logic-tables节点并对BindingTableRuleConfig对象的tableNames属性赋值
            bindingTableRuleFactory.addPropertyValue("tableNames", bindingTableRuleElement.getAttribute(ShardingJdbcDataSourceBeanDefinitionParserTag.LOGIC_TABLES_ATTRIBUTE));
            result.add(bindingTableRuleFactory.getBeanDefinition());
        }
        return result;
    }
    
    private BeanDefinition parseDefaultDatabaseStrategyConfig(final Element element) {
        return parseDefaultStrategyConfig(element, ShardingJdbcDataSourceBeanDefinitionParserTag.DEFAULT_DATABASE_STRATEGY_ATTRIBUTE);
    }
    
    private BeanDefinition parseDefaultTableStrategyConfig(final Element element) {
        return parseDefaultStrategyConfig(element, ShardingJdbcDataSourceBeanDefinitionParserTag.DEFAULT_TABLE_STRATEGY_ATTRIBUTE);
    }
    
    private BeanDefinition parseDefaultStrategyConfig(final Element element, final String attr) {
        Element strategyElement = DomUtils.getChildElementByTagName(element, attr);
        return null == strategyElement ? null : ShardingJdbcStrategyBeanDefinition.getBeanDefinitionByElement(strategyElement);
    }
    
    private Properties parseProperties(final Element element, final ParserContext parserContext) {
        Element propsElement = DomUtils.getChildElementByTagName(element, ShardingJdbcDataSourceBeanDefinitionParserTag.PROPS_TAG);
        return null == propsElement ? new Properties() : parserContext.getDelegate().parsePropsElement(propsElement);
    }
}
