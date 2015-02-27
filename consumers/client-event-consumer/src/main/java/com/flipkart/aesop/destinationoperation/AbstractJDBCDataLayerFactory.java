/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 *******************************************************************************/

package com.flipkart.aesop.destinationoperation;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Abstract Data Layer Factory for the factories which create data layers which use JDBC to interact with the
 * destination store.
 * @author Prakhar Jain
 * @param <T> Destination Data Layer
 */
public abstract class AbstractJDBCDataLayerFactory<T extends JDBCDataLayer> implements FactoryBean<T>
{
	/** Data Source Map with Namespace as key and Data Source as value. */
	private Map<String, DataSource> dataSourceMap;

	/**
	 * Creates Destination Later using the JDBC Template.
	 * @param jdbcTemplateMap
	 * @return Destination Data Layer
	 */
	public abstract T createDestinationOperationHandler(Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap);

	public T getObject() throws Exception
	{
		Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap = new HashMap<String, NamedParameterJdbcTemplate>();
		for (Map.Entry<String, DataSource> dataSource : dataSourceMap.entrySet())
		{
			jdbcTemplateMap.put(dataSource.getKey(),
			        new NamedParameterJdbcTemplate(dataSourceMap.get(dataSource.getKey())));
		}
		return createDestinationOperationHandler(jdbcTemplateMap);
	}

	public abstract Class<T> getObjectType();

	public boolean isSingleton()
	{
		return true;
	}

	/**
	 * Sets the Data Source Map.
	 * @param dataSourceMap
	 */
	public void setDataSourceMap(Map<String, DataSource> dataSourceMap)
	{
		this.dataSourceMap = dataSourceMap;
	}
}
