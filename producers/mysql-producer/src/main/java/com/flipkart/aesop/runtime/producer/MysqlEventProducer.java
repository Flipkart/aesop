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
package com.flipkart.aesop.runtime.producer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.avro.MysqlAvroEventManager;
import com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.runtime.producer.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.runtime.producer.mapper.BinLogEventMapper;
import com.flipkart.aesop.runtime.producer.schema.eventprocessor.SchemaChangeEventProcessor;
import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;
import com.flipkart.aesop.runtime.producer.txnprocessor.MysqlTransactionManager;
import com.flipkart.aesop.runtime.producer.txnprocessor.impl.MysqlTransactionManagerImpl;
import com.flipkart.aesop.runtime.producer.txnprocessor.impl.NaiveSCNGenerator;
import com.google.code.or.OpenReplicator;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * <code>MysqlEventProducer</code> kick starts bin log event listener to listen to Mysql events using open replicator
 * library and in
 * turn creates change events of {@link GenericRecord}
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class MysqlEventProducer<T extends GenericRecord> extends AbstractEventProducer implements InitializingBean
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(MysqlEventProducer.class);
	/** Default Mysql port */
	private static final Integer DEFAULT_MYSQL_PORT = 3306;
	/** Pattern for extracting /3306/mysql-bin out of mysql://or_test%2For_test@localhost:3306/3306/mysql-bin */
	private static final Pattern PATH_PATTERN = Pattern.compile("/([0-9]+)/[a-z|A-Z|0-9|-]+");
	/** Index of server id in pattern match group */
	private static final int SERVER_ID = 1;
	/** Index of bin log prefix in pattern match group */
	private static final int BIN_LOG_PREFIX = 2;
	/** Event mapper which maps bin log events to one of schemas registered */
	protected Map<Integer, BinLogEventMapper<T>> binLogEventMappers;
	/** Open Replicator which listens and parses bin log events from Mysql */
	protected OpenReplicator openReplicator;
	/** Mysql transaction manager */
	protected MysqlTransactionManager mysqlTxnManager;
	/** Event Id to Event Processor map */
	protected Map<Integer, BinLogEventProcessor> eventsMap;
	/** Schema Change Event Processor */
	protected SchemaChangeEventProcessor schemaChangeEventProcessor;
	/** The SCN generator implementation, initialized to the default simple implementation*/
	protected SCNGenerator scnGenerator = new NaiveSCNGenerator();

	/**
	 * Interface method implementation. Checks for mandatory dependencies and creates the Open Replicator
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception
	{
		Assert.notNull(this.binLogEventMappers,
		        "'binLogEventMapper' cannot be null. No bin log event mapper found. This Mysql Events producer will not be initialized");
		Assert.notNull(this.eventsMap,
		        "'eventsMap' cannot be null. eventsMap is not initialized properly.This Mysql Events producer will not be initialized");
	}

	/**
	 * Starting point for this event producer. Starts Open Replicator listener
	 * @param sinceSCN starting SCN
	 * @see com.linkedin.databus2.producers.EventProducer#start(long)
	 */
	public void start(long sinceSCN)
	{
		this.sinceSCN.set(sinceSCN);
		openReplicator = new OpenReplicator();
		String binlogFile;
		try
		{
			String binlogFilePrefix = processUri(new URI(physicalSourceStaticConfig.getUri()));
			int offset = offset(sinceSCN);
			int logid = logid(sinceSCN);
			LOGGER.debug("SCN : " + sinceSCN + " logid : " + logid);
			binlogFile = String.format("%s.%06d", binlogFilePrefix, logid);
			LOGGER.debug("Bin Log File Name : " + binlogFile);
			Map<String, Short> tableUriToSrcIdMap = new HashMap<String, Short>();
			Map<String, String> tableUriToSrcNameMap = new HashMap<String, String>();
			Map<Integer, MysqlAvroEventManager<T>> eventManagersMap = new HashMap<Integer, MysqlAvroEventManager<T>>();
			for (LogicalSourceStaticConfig sourceConfig : physicalSourceStaticConfig.getSources())
			{
				tableUriToSrcIdMap.put(sourceConfig.getUri().toLowerCase(), sourceConfig.getId());
				tableUriToSrcNameMap.put(sourceConfig.getUri().toLowerCase(), sourceConfig.getName());
				MysqlAvroEventManager<T> manager = null;
				try
				{
					manager = buildEventManagers(sourceConfig, physicalSourceStaticConfig);
				}
				catch (Exception ex)
				{
					LOGGER.error("Got exception while building monitored sources for config :" + sourceConfig, ex);
					throw new InvalidConfigException(ex);
				}
				eventManagersMap.put(Integer.valueOf(sourceConfig.getId()), manager);
			}
			schemaChangeEventProcessor.setSchemaRegistryService(schemaRegistryService);
			schemaChangeEventProcessor.setTableUriToSrcNameMap(tableUriToSrcNameMap);

			/** updating schemas for registered logical sources */
			for (LogicalSourceStaticConfig sourceConfig : physicalSourceStaticConfig.getSources())
			{
				String[] parts = sourceConfig.getUri().split("\\.");
				schemaChangeEventProcessor.process(parts[0], parts[1]);
			}

			mysqlTxnManager =
			        new MysqlTransactionManagerImpl<T>(eventBuffer, maxScnReaderWriter, dbusEventsStatisticsCollector,
			                eventManagersMap, logid, tableUriToSrcIdMap, tableUriToSrcNameMap, schemaRegistryService,
			                this.sinceSCN, binLogEventMappers, scnGenerator, this);
			mysqlTxnManager.setShutdownRequested(false);
			OpenReplicationListener orl =
			        new OpenReplicationListener(mysqlTxnManager, eventsMap, schemaChangeEventProcessor,
			                binlogFilePrefix);
			openReplicator.setBinlogFileName(binlogFile);
			openReplicator.setBinlogPosition(offset);
			openReplicator.setBinlogEventListener(orl);
			openReplicator.start();
		}
		catch (URISyntaxException u)
		{
			LOGGER.error("Exception occurred while processing uri : " + u);
			return;
		}
		catch (InvalidConfigException e)
		{
			LOGGER.error("Exception occurred while processing uri : " + e);
			return;
		}
		catch (Exception e)
		{
			LOGGER.error("Error occurred while starting open replication.." + e);
			return;
		}
		LOGGER.info("Open Replicator has been started successfully for the file " + binlogFile);
	}

	/**
	 * Builds event managers for the given sources.
	 * @param sourceConfig logical source configuration
	 * @param pConfig physical source configuration
	 * @return event factory for the given source
	 * @throws DatabusException Generic Databus Exception
	 * @throws EventCreationException Thrown when event creation failed for a databus source
	 * @throws UnsupportedKeyException Thrown when the data type of the "key" field is not a supported type
	 * @throws InvalidConfigException Throws when invalid source config is present in configuration provided
	 */
	public MysqlAvroEventManager<T> buildEventManagers(LogicalSourceStaticConfig sourceConfig,
	        PhysicalSourceStaticConfig pConfig) throws DatabusException, EventCreationException,
	        UnsupportedKeyException, InvalidConfigException
	{
		MysqlAvroEventManager<T> manager = new MysqlAvroEventManager<T>(sourceConfig.getId(), (short) pConfig.getId());
		return manager;
	}

	/**
	 * Returns the logid ( upper 32 bits of the SCN )
	 * For e.g., mysql-bin.000001 is said to have an id 000001
	 * @param scn system change number
	 * @return logid
	 */
	public static int logid(long scn)
	{
		if (scn == -1 || scn == 0)
		{
			return 1;
		}
		return (int) ((scn >> 32) & 0xFFFFFFFF);
	}

	/**
	 * Returns the binlogoffset ( lower 32 bits of the SCN )
	 * @param scn system change number
	 * @return binlogoffset
	 */
	public static int offset(long scn)
	{
		if (scn == -1 || scn == 0)
		{
			return 4;
		}
		return (int) (scn & 0xFFFFFFFF);
	}

	/**
	 * Interface method implementation. Returns {@link BinLogEventMapper#getUniqueName()}
	 * @see com.linkedin.databus2.producers.EventProducer#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}

	/**
	 * Interface method implementation.
	 * @see com.linkedin.databus2.producers.EventProducer#getSCN()
	 */
	public long getSCN()
	{
		return this.sinceSCN.get();
	}
	
	public void updateSCN(long latestScn)
	{
		this.sinceSCN.set(latestScn);
	}

	/**
	 * Interface method implementation. Returns inverted status of {@link #isRunning()}
	 * @see com.linkedin.databus2.producers.EventProducer#isPaused()
	 */
	@Override
	public boolean isPaused()
	{
		return !this.openReplicator.isRunning();
	}

	/**
	 * Interface method implementation. Returns {@link OpenReplicator#isRunning()}
	 * @see com.linkedin.databus2.producers.EventProducer#isRunning()
	 */
	@Override
	public boolean isRunning()
	{
		return this.openReplicator.isRunning();
	}

	/**
	 * Interface method implementation. Stops the Open Replicator
	 * @see com.linkedin.databus2.producers.EventProducer#shutdown()
	 */
	@Override
	public void shutdown()
	{
		LOGGER.info("Shutdown has been requested. MYSQLEventProducer shutttng down");
		try
		{
			mysqlTxnManager.setShutdownRequested(true);
			LOGGER.info("Open Replicator Shutting down");
			this.openReplicator.stop(10, TimeUnit.SECONDS);
			LOGGER.info("Open Replicator shutdown complete");
			super.shutdown();
		}
		catch (Exception e)
		{
			LOGGER.error("Error while stopping open replicator", e);
		}
		LOGGER.info("MYSQLEventProducer shutdown completed");
	}
	
	/**
	 * Returns the host from where the bin log is being read
	 * @return the bin log host name
	 */
	public String getBinLogHost() {
		return this.openReplicator.getHost();
	}

	/** Methods that are not supported and therefore throw {@link UnsupportedOperationException} */
	public void pause()
	{
		throw new UnsupportedOperationException("'pause' is not supported on this event producer");
	}

	public void unpause()
	{
		throw new UnsupportedOperationException("'unpause' is not supported on this event producer");
	}

	public void waitForShutdown() throws InterruptedException, IllegalStateException
	{
		throw new UnsupportedOperationException("'waitForShutdown' is not supported on this event producer");
	}

	public void waitForShutdown(long time) throws InterruptedException, IllegalStateException
	{
		throw new UnsupportedOperationException("'waitForShutdown(long time)' is not supported on this event producer");
	}

	/** Getters and Setters for this class */
	public Map<Integer, BinLogEventMapper<T>> getBinLogEventMappers(){
		return binLogEventMappers;
	}
	public void setBinLogEventMappers(Map<Integer, BinLogEventMapper<T>> binLogEventMapper){
		this.binLogEventMappers = binLogEventMapper;
	}
	public MysqlTransactionManager getMysqlTxnManager(){
		return mysqlTxnManager;
	}
	public void setMysqlTxnManager(MysqlTransactionManager mysqlTxnManager){
		this.mysqlTxnManager = mysqlTxnManager;
	}
	public Map<Integer, BinLogEventProcessor> getEventsMap(){
		return eventsMap;
	}
	public void setEventsMap(Map<Integer, BinLogEventProcessor> eventsMap){
		this.eventsMap = eventsMap;
	}
	public SchemaChangeEventProcessor getSchemaChangeEventProcessor(){
		return schemaChangeEventProcessor;
	}
	public void setSchemaChangeEventProcessor(SchemaChangeEventProcessor schemaChangeEventProcessor){
		this.schemaChangeEventProcessor = schemaChangeEventProcessor;
	}
	public SCNGenerator getScnGenerator() {
		return scnGenerator;
	}
	public void setScnGenerator(SCNGenerator scnGenerator) {
		this.scnGenerator = scnGenerator;
	}

	/**
	 * Extracts individual attributes such as username, password, hostname, port ,server id etc from the uri of the
	 * format mysql://or_test%2For_test@localhost:3306/3306/mysql-bin
	 * @param uri uri of the format mysql://or_test%2For_test@localhost:3306/3306/mysql-bin
	 * @return returns bin log prefix
	 */
	protected String processUri(URI uri) throws InvalidConfigException
	{
		String userInfo = uri.getUserInfo();
		if (null == userInfo)
		{
			String errorMessage = "missing user info in: " + uri;
			LOGGER.error(errorMessage);
			throw new InvalidConfigException(errorMessage);
		}
		int slashPos = userInfo.indexOf('/');
		if (slashPos < 0)
		{
			slashPos = userInfo.length();
		}
		else if (0 == slashPos)
		{
			String errorMessage = "missing user name in user info: " + userInfo;
			LOGGER.error(errorMessage);
			throw new InvalidConfigException(errorMessage);
		}
		String userName = userInfo.substring(0, slashPos);
		String userPass = slashPos < userInfo.length() - 1 ? userInfo.substring(slashPos + 1) : null;
		String hostName = uri.getHost();

		int port = uri.getPort();
		if (port < 0)
			port = DEFAULT_MYSQL_PORT;

		String path = uri.getPath();
		if (null == path)
		{
			String errorMessage = "missing path: " + uri;
			LOGGER.error(errorMessage);
			throw new InvalidConfigException(errorMessage);
		}
		Matcher matcher = PATH_PATTERN.matcher(path);
		if (!matcher.matches())
		{
			String errorMessage = "invalid path:" + path;
			LOGGER.error(errorMessage);
			throw new InvalidConfigException(errorMessage);
		}
		String[] group = matcher.group().split("/");
		if (group.length != 3)
		{
			String errorMessage = "Invalid format " + Arrays.toString(group);
			LOGGER.error(errorMessage);
			throw new InvalidConfigException(errorMessage);
		}
		String serverIdStr = group[SERVER_ID];

		int serverId = -1;
		try
		{
			serverId = Integer.parseInt(serverIdStr);
		}
		catch (NumberFormatException e)
		{
			String errorMessage = "incorrect mysql serverid:" + serverId;
			LOGGER.error(errorMessage);
			throw new InvalidConfigException(errorMessage);
		}

		/** Assign them to incoming variables */
		if (null != openReplicator)
		{
			openReplicator.setUser(userName);
			if (null != userPass)
			{
				openReplicator.setPassword(userPass);
			}
			openReplicator.setHost(hostName);
			openReplicator.setPort(port);
			openReplicator.setServerId(serverId);
		}
		LOGGER.debug("Extracted bin log prefix is " + group[BIN_LOG_PREFIX]);
		return group[BIN_LOG_PREFIX];
	}

}
