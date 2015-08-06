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

	private Properties dataSourceProperties;
	private String driverClass;
	private String jdbcUrl;
	private Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap;
	public static final Logger LOGGER = LogFactory
			.getLogger(AbstractHBaseDataLayerFactory.class);

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
		NamedParameterJdbcTemplate upsertNamedParameterJdbcTemplate = new NamedParameterJdbcTemplate(
				dataSource);
		NamedParameterJdbcTemplate deleteNamedParameterJdbcTemplate = new NamedParameterJdbcTemplate(
				dataSource);
		jdbcTemplateMap.put(
				com.linkedin.databus.core.DbusOpcode.UPSERT.toString(),
				upsertNamedParameterJdbcTemplate);
		jdbcTemplateMap.put(
				com.linkedin.databus.core.DbusOpcode.DELETE.toString(),
				deleteNamedParameterJdbcTemplate);

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

}
