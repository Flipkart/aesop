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
package com.flipkart.aesop.hbasedatalayer;

import java.beans.PropertyVetoException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.JDBCDataLayer;
import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * This class was introduced to create separate JDBC pool for all consumers.
 * <code>ComboPooledDataSource</code> always initializes with default
 * configuration in case of Embedded driver.
 * 
 * @author dhirendra.singh
 *
 * @param <T>
 *            Implementation of <code>JDBCDataLayer</code>.
 */
public abstract class AbstractHBaseDataLayerFactory<T extends JDBCDataLayer>
		implements FactoryBean<T> {
	/** c3p0 properties to be used for creating jdbcTemplate **/
	private Properties dataSourceProperties;
	/** Name of driver class to be used for creating jdbcTemplate **/
	private String driverClass;
	/** jdbc url of hbase **/
	private String jdbcUrl;
	/** name of data source to be used for wirtes **/
	private String dataSourceName;
	/**
	 * JDBC Template Map, with Namespace name as key and the corresponding JDBC
	 * template as value.
	 **/
	private Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap;
	/** Logger for this class. */
	public static final Logger LOGGER = LogFactory
			.getLogger(AbstractHBaseDataLayerFactory.class);

	/**
	 * Creates Destination Later using the JDBC Template.
	 * 
	 * @param jdbcTemplateMap
	 * @return Destination Data Layer
	 */
	public abstract T createDestinationOperationHandler(
			Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap);

	public T getObject() throws Exception {
		createJdbcTemplateMap();
		return createDestinationOperationHandler(jdbcTemplateMap);
	}

	public abstract Class<?> getObjectType();

	/**
	 * For each consumer this would return a new instance of this Factory.
	 */
	public boolean isSingleton() {
		return false;
	}

	private void createJdbcTemplateMap() {
		jdbcTemplateMap = new HashMap<String, NamedParameterJdbcTemplate>();
		DataSource dataSource = getDataSource();
		NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(
				dataSource);
		jdbcTemplateMap.put(getDataSourceName(), namedParameterJdbcTemplate);

	}

	private DataSource getDataSource() {
		ComboPooledDataSource comboPooledDataSource = null;
		try {
			comboPooledDataSource = new ComboPooledDataSource();
			comboPooledDataSource.setDriverClass(getDriverClass());
			comboPooledDataSource.setJdbcUrl(getJdbcUrl());
			comboPooledDataSource.setProperties(getDataSourceProperties());
		} catch (PropertyVetoException e) {
			LOGGER.error(e.getMessage());
		}

		return comboPooledDataSource;

	}

	public Properties getDataSourceProperties() {
		return dataSourceProperties;
	}

	public void setDataSourceProperties(Properties dataSourceProperties) {
		this.dataSourceProperties = dataSourceProperties;
	}

	public String getDriverClass() {
		return driverClass;
	}

	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getDataSourceName() {
		return dataSourceName;
	}

	public void setDataSourceName(String dataSourceName) {
		this.dataSourceName = dataSourceName;
	}

}
