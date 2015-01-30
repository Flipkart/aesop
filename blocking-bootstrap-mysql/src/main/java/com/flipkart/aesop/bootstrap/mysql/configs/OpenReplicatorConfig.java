package com.flipkart.aesop.bootstrap.mysql.configs;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public class OpenReplicatorConfig
{
	private long startFileNumber;
	private long endFileNumber;
	private String binlogPrefix;
	private String userName;
	private String password;
	private int port;
	private String host;
	private int serverId;
	private long binlogPosition;

	public long getStartFileNumber()
	{
		return startFileNumber;
	}

	public void setStartFileNumber(long startFileNumber)
	{
		this.startFileNumber = startFileNumber;
	}

	public long getEndFileNumber()
	{
		return endFileNumber;
	}

	public void setEndFileNumber(long endFileNumber)
	{
		this.endFileNumber = endFileNumber;
	}

	public String getBinlogPrefix()
	{
		return binlogPrefix;
	}

	public void setBinlogPrefix(String binlogPrefix)
	{
		this.binlogPrefix = binlogPrefix;
	}

	public String getUserName()
	{
		return userName;
	}

	public void setUserName(String userName)
	{
		this.userName = userName;
	}

	public String getPassword()
	{
		return password;
	}

	public void setPassword(String password)
	{
		this.password = password;
	}

	public int getPort()
	{
		return port;
	}

	public void setPort(int port)
	{
		this.port = port;
	}

	public String getHost()
	{
		return host;
	}

	public void setHost(String host)
	{
		this.host = host;
	}

	public int getServerId()
	{
		return serverId;
	}

	public void setServerId(int serverId)
	{
		this.serverId = serverId;
	}

	public long getBinlogPosition()
	{
		return binlogPosition;
	}

	public void setBinlogPosition(long binlogPosition)
	{
		this.binlogPosition = binlogPosition;
	}
}
