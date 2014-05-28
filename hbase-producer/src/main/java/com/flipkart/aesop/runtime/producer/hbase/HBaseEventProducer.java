/*
 * Copyright 2012-2015, the original author or authors.
 *
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
 */
package com.flipkart.aesop.runtime.producer.hbase;

import java.net.InetAddress;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.AbstractEventProducer;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;

/**
 * <code>HBaseEventProducer</code> is a sub-type of {@link AbstractEventProducer} that listens to HBase WAL edits using the hbase-sep module library classes 
 * such as {@link SepConsumer} and {@link EventListener} and in turn creates change events of {@link GenericRecord} sub-type T.
 *
 * @author Regunath B
 * @version 1.0, 28 Jan 2014
 */
public class HBaseEventProducer<T extends GenericRecord> extends AbstractEventProducer implements InitializingBean {
	
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(HBaseEventProducer.class);
	
	/** The HBase replication configuration parameter */
	private static final String HBASE_REPLICATION_CONFIG = "hbase.replication";
	
	/** The default number of worker threads that process WAL edit events*/
	private static final int WORKER_THREADS = 1;

	/** The SEP consumer instance initialized by this Producer*/
	protected SepConsumer sepConsumer;
	
	/** Host name where this producer is running i.e. local host name*/
	private String localHost;
	
	/** The Zookeeper connection properties*/
	protected String zkQuorum;
	protected Integer zkPort;
	
	/** The number of WAL edits processing worker threads*/
	protected int workerThreads = WORKER_THREADS;
	
	/** The SepEventMapper for translating WAL edits to change events*/
	protected SepEventMapper<T> sepEventMapper;

	/**
	 * Interface method implementation. Checks for mandatory dependencies and creates the SEP consumer
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.zkQuorum,"'zkQuorum' cannot be null. Zookeeper quorum list must be specified. This HBase Events producer will not be initialized");
		Assert.notNull(this.zkPort,"'zkPort' cannot be null. Zookeeper port must be specified. This HBase Events producer will not be initialized");		
		Assert.notNull(this.sepEventMapper,"'sepEventMapper' cannot be null. No WAL edits event mapper found. This HBase Events producer will not be initialized");		
		this.localHost = InetAddress.getLocalHost().getHostName();
	}
	
	/**
	 * Interface method implementation. Starts up the SEP consumer
	 * @see com.linkedin.databus2.producers.EventProducer#start(long)
	 */
	public void start (long sinceSCN) {
		this.sinceSCN.set(sinceSCN);
		LOGGER.info("Starting SEP subscription : " + this.getName());
		LOGGER.info("ZK connection details [host:port] = {} : {}", this.zkQuorum, this.zkPort);
		LOGGER.info("Using hostname to bind to : " + this.localHost);
		LOGGER.info("Using worker threads : " + this.workerThreads);
		LOGGER.info("Listening to WAL edits from : " + this.sinceSCN);
		try {
	        Configuration conf = HBaseConfiguration.create();
	        // enable replication to get WAL edits
	        conf.setBoolean(HBASE_REPLICATION_CONFIG, true);

	        ZooKeeperItf zk = ZkUtil.connect(this.zkQuorum, this.zkPort);
	        SepModel sepModel = new SepModelImpl(zk, conf);

	        final String subscriptionName = this.getName();

	        if (!sepModel.hasSubscription(subscriptionName)) {
	            sepModel.addSubscriptionSilent(subscriptionName);
	        }
	        this.sepConsumer = new SepConsumer(subscriptionName, this.sinceSCN.get(), new RelayAppender(), this.workerThreads, this.localHost, zk, conf);							
			this.sepConsumer.start();
		} catch (Exception e) {
			LOGGER.error("Error starting WAL edits consumer. Producer not started!. Error message : " + e.getMessage(), e);
		}
	}
	
	/**
	 * The SEP EventListener that consumes {@link SepEvent} instances and appends them to the Databus event buffer after suitable conversion/interpretation.
	 */
	class RelayAppender implements EventListener {
		public void processEvents(List<SepEvent> sepEvents) {
			long lastSavedSCN = sinceSCN.get();
			eventBuffer.startEvents();
            for (SepEvent sepEvent : sepEvents) {
            	T changeEvent = sepEventMapper.mapSepEvent(sepEvent);
            	byte[] schemaId=SchemaHelper.getSchemaId(changeEvent.getSchema().toString());
               	byte[] serializedEvent = serializeEvent(changeEvent);
            	// we find the last processed timestamp and are conservative to take the earliest
            	long latestTimestamp = 0;
            	for (KeyValue kv : sepEvent.getKeyValues()) {
            		latestTimestamp = Math.max(latestTimestamp, kv.getTimestamp());
            	}
				DbusEventKey eventKey = new DbusEventKey(sepEvent.getRow()); // we use the SepEvent row key as the identifier
				DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,latestTimestamp,
						(short)physicalSourceStaticConfig.getId(),(short)physicalSourceStaticConfig.getId(),
						System.nanoTime(),(short)physicalSourceStaticConfig.getSources()[0].getId(), // here we use the Logical Source Id
						schemaId,serializedEvent, false, true);
				eventBuffer.appendEvent(eventKey, eventInfo, dbusEventsStatisticsCollector);    
				sinceSCN.set(Math.max(lastSavedSCN, latestTimestamp));
            }
            eventBuffer.endEvents(sinceSCN.get() , dbusEventsStatisticsCollector);
			try {
				maxScnReaderWriter.saveMaxScn(sinceSCN.get());
			} catch (DatabusException e) {
				LOGGER.error("Unable to persist last processed SCN. SCN value is stale. Error is : " + e.getMessage(), e);
				throw new PlatformException("Unable to write last processed SCN to log. Signalling for re-delivery of WAL edits from : " + lastSavedSCN);
			} 
			LOGGER.debug("Processed SEP event count : " + sepEvents.size());
		}		 	        	
    }
	
	/**
	 * Interface method implementation. Stops the SEP consumer
	 * @see com.linkedin.databus2.producers.EventProducer#shutdown()
	 */
	public void shutdown() {
		this.sepConsumer.stop();
	}

	/**
	 * Interface method implementation. Returns inverted status of {@link #isRunning()}
	 * @see com.linkedin.databus2.producers.EventProducer#isPaused()
	 */
	public boolean isPaused() {
		return !this.isRunning();
	}

	/**
	 * Interface method implementation. Returns {@link SepConsumer#isRunning()}
	 * @see com.linkedin.databus2.producers.EventProducer#isRunning()
	 */
	public boolean isRunning() {
		return this.sepConsumer.isRunning();
	}

	/** Methods that are not supported and therefore throw {@link UnsupportedOperationException}*/
	public void pause() {throw new UnsupportedOperationException("'pause' is not supported on this event producer");}
	public void unpause() {throw new UnsupportedOperationException("'unpause' is not supported on this event producer");}
	public void waitForShutdown() throws InterruptedException,IllegalStateException {throw new UnsupportedOperationException("'waitForShutdown' is not supported on this event producer");}
	public void waitForShutdown(long time) throws InterruptedException,IllegalStateException {throw new UnsupportedOperationException("'waitForShutdown(long time)' is not supported on this event producer");}

	/** Start Setter/Getter methods*/
	public String getZkQuorum() {
		return zkQuorum;
	}
	public void setZkQuorum(String zkQuorum) {
		this.zkQuorum = zkQuorum;
	}
	public Integer getZkPort() {
		return zkPort;
	}
	public void setZkPort(Integer zkPort) {
		this.zkPort = zkPort;
	}
	public int getWorkerThreads() {
		return workerThreads;
	}
	public void setWorkerThreads(int workerThreads) {
		this.workerThreads = workerThreads;
	}
	public SepEventMapper<T> getSepEventMapper() {
		return sepEventMapper;
	}
	public void setSepEventMapper(SepEventMapper<T> sepEventMapper) {
		this.sepEventMapper = sepEventMapper;
	}
	/** End Setter/Getter methods*/

}
