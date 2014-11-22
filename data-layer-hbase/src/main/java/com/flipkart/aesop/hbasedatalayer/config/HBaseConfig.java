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
package com.flipkart.aesop.hbasedatalayer.config;

/**
 * HBase config parameters pojo.
 * @author Prakhar Jain
 */
public class HBaseConfig
{
	/* hbase.zookeeper.quorum=172.17.228.148
	   hbase.zookeeper.property.clientPort=2181
	   zookeeper.znode.parent=/hbase
	*/

	/** Phoenix JDBC URL Prefix. */
	private static final String PHOENIX_JDBC_URL_PREFIX = "jdbc:phoenix:";
	/** HBase Zookeeper Quorum. */
	private final String hBaseZookeeperQuorum;
	/** HBase Zookeeper Client Port. Optional. If "DEFAULT" then 2181 is used. */
	private String hBaseZookeeperClientPort;
	/** Zookeeper Znode Parent. Optional. If "DEFAULT", no znode parent is used. */
	private String zookeeperZnodeParent;

	/**
	 * Constructor with only mandatory parameter. Use setters for setting other optional parameters.
	 * @param hBaseZookeeperQuorum
	 */
	public HBaseConfig(String hBaseZookeeperQuorum)
	{
		super();
		this.hBaseZookeeperQuorum = hBaseZookeeperQuorum;
	}

	/**
	 * Sets HBase Zookeeper Client Port.
	 * @param hBaseZookeeperClientPort
	 */
	public void sethBaseZookeeperClientPort(String hBaseZookeeperClientPort)
	{
		this.hBaseZookeeperClientPort = hBaseZookeeperClientPort;
	}

	/**
	 * Sets Zookeeper Znode Parent.
	 * @param zookeeperZnodeParent
	 */
	public void setZookeeperZnodeParent(String zookeeperZnodeParent)
	{
		this.zookeeperZnodeParent = zookeeperZnodeParent;
	}

	/**
	 * Generates Connection URL using the config params.
	 * @return Connection URL.
	 */
	public String getConnectionUrl()
	{
		StringBuilder connectionUrlBuilder = new StringBuilder();

		connectionUrlBuilder.append(PHOENIX_JDBC_URL_PREFIX);
		connectionUrlBuilder.append(hBaseZookeeperQuorum);

		if (hBaseZookeeperClientPort != null && !"DEFAULT".equalsIgnoreCase(hBaseZookeeperClientPort))
		{
			connectionUrlBuilder.append(":" + hBaseZookeeperClientPort);
		}
		if (zookeeperZnodeParent != null && !"DEFAULT".equalsIgnoreCase(zookeeperZnodeParent))
		{
			connectionUrlBuilder.append(":" + zookeeperZnodeParent);
		}

		return connectionUrlBuilder.toString();
	}
}
