package com.flipkart.aesop.avro.schemagenerator.mysql;

/**
 * <code> DBConfig </code> is the configuration object for the dataSource initialization
 * Created by chandan.bansal on 12/05/14.
 * @author chandan.bansal
 */
public class DataSourceConfig
{

	/** The db name. */
	public String dbName;

	/** The host name. */
	public String hostName;

	/** The user name. */
	public String userName;

	/** The password. */
	public String password;

	/** The port. */
	public String port = "3306";

	/** The min pool size. */
	public int minPoolSize = 1;

	/** The max pool size. */
	public int maxPoolSize = 5;

	/**
	 * Instantiates a new dB config.
	 * @param dbName the db name
	 * @param hostName the host name
	 * @param userName the user name
	 * @param password the password
	 * @param port the port
	 * @param minPoolSize the min pool size
	 * @param maxPoolSize the max pool size
	 */
	public DataSourceConfig(String dbName, String hostName, String userName, String password, String port, int minPoolSize,
	        int maxPoolSize)
	{
		super();
		this.dbName = dbName;
		this.hostName = hostName;
		this.userName = userName;
		this.password = password;
		this.port = port;
		this.minPoolSize = minPoolSize;
		this.maxPoolSize = maxPoolSize;
	}

	/**
	 * Instantiates a new dB config.
	 */
	public DataSourceConfig()
	{
	}

	/**
	 * Gets the host name.
	 * @return the host name
	 */
	public String getHostName()
	{
		return hostName;
	}

	/**
	 * Sets the host name.
	 * @param hostName the new host name
	 */
	public void setHostName(String hostName)
	{
		this.hostName = hostName;
	}

	/**
	 * Gets the user name.
	 * @return the user name
	 */
	public String getUserName()
	{
		return userName;
	}

	/**
	 * Sets the user name.
	 * @param userName the new user name
	 */
	public void setUserName(String userName)
	{
		this.userName = userName;
	}

	/**
	 * Gets the password.
	 * @return the password
	 */
	public String getPassword()
	{
		return password;
	}

	/**
	 * Sets the password.
	 * @param password the new password
	 */
	public void setPassword(String password)
	{
		this.password = password;
	}

	/**
	 * Gets the port.
	 * @return the port
	 */
	public String getPort()
	{
		return port;
	}

	/**
	 * Sets the port.
	 * @param port the new port
	 */
	public void setPort(String port)
	{
		this.port = port;
	}

	/**
	 * Gets the min pool size.
	 * @return the min pool size
	 */
	public int getMinPoolSize()
	{
		return minPoolSize;
	}

	/**
	 * Sets the min pool size.
	 * @param minPoolSize the new min pool size
	 */
	public void setMinPoolSize(int minPoolSize)
	{
		this.minPoolSize = minPoolSize;
	}

	/**
	 * Gets the max pool size.
	 * @return the max pool size
	 */
	public int getMaxPoolSize()
	{
		return maxPoolSize;
	}

	/**
	 * Sets the max pool size.
	 * @param maxPoolSize the new max pool size
	 */
	public void setMaxPoolSize(int maxPoolSize)
	{
		this.maxPoolSize = maxPoolSize;
	}

	/**
	 * Gets the db name.
	 * @return the db name
	 */
	public String getDbName()
	{
		return dbName;
	}

	/**
	 * Sets the db name.
	 * @param dbName the new db name
	 */
	public void setDbName(String dbName)
	{
		this.dbName = dbName;
	}

	/**
	 * Gets the jDBC string.
	 * @return the jDBC string
	 */
	public String getJDBCString()
	{
		return "jdbc:mysql://" + hostName + ":" + port + "/" + dbName + "?zeroDateTimeBehavior=convertToNull&user="
		        + userName + "&password=" + password;
	}
}
