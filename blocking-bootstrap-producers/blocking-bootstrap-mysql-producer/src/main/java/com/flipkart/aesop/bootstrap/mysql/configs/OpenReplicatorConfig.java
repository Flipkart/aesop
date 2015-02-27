/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.aesop.bootstrap.mysql.configs;

/**
 * <code>OpenReplicatorConfig</code> holds the configuration data for {@link com.google.code.or.OpenReplicator}
 * @author nrbafna
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
