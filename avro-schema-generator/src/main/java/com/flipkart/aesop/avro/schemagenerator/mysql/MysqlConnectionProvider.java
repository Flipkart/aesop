package com.flipkart.aesop.avro.schemagenerator.mysql;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.management.RuntimeErrorException;
import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * <code> MysqlConnectionProvider </code> provides database connection object for the given dataSourceId.
 * @author chandan.bansal
 */
public class MysqlConnectionProvider
{

	/** The Constant DEFAULT_MYSQL_JDBC_DRIVER. */
	private static final String DEFAULT_MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";

	/** The data sources. */
	private static ConcurrentMap<String, DataSource> dataSources = new ConcurrentHashMap<String, DataSource>();

	/** The instance. */
	private static MysqlConnectionProvider instance = new MysqlConnectionProvider();

	/**
	 * Gets the single instance of MysqlConnectionProvider.
	 * @return single instance of MysqlConnectionProvider
	 */
	public static MysqlConnectionProvider getInstance()
	{
		return instance;
	}

	/**
	 * Setup data source.
	 * @param dataSourceConfig the data source config
	 * @return the data source
	 * @throws PropertyVetoException the property veto exception
	 */
	private DataSource setupDataSource(DataSourceConfig dataSourceConfig) throws PropertyVetoException
	{
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		cpds.setDriverClass(DEFAULT_MYSQL_JDBC_DRIVER);
		cpds.setJdbcUrl(dataSourceConfig.getJDBCString());
		cpds.setMinPoolSize(dataSourceConfig.getMinPoolSize());
		cpds.setMaxPoolSize(dataSourceConfig.getMaxPoolSize());
		cpds.setTestConnectionOnCheckin(true);
		return cpds;
	}

	/**
	 * Adds the data source.
	 * @param dataSourceId the data source id
	 * @param dataSourceConfig the data source config
	 * @throws PropertyVetoException the property veto exception
	 */
	public void addDataSource(DataSourceConfig dataSourceConfig) throws PropertyVetoException
	{
		dataSources.putIfAbsent(dataSourceConfig.getDbName(), setupDataSource(dataSourceConfig));
	}

	/**
	 * Gets the connection.
	 * @param dataSourceId the data source id
	 * @return the connection
	 * @throws SQLException the sQL exception
	 */
	public Connection getConnection(String dataSourceId) throws SQLException
	{
		if (dataSources.containsKey(dataSourceId))
		{
			return dataSources.get(dataSourceId).getConnection();
		}
		else
		{
			throw new RuntimeErrorException(null, "No dataSouce found for dataSourceId : " + dataSourceId);
		}
	}

}
