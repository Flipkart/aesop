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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;
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
	private static final String ZK_QUORUM_CONFIG = "hbase.zookeeper.quorum";
	private static final String ZK_CLIENT_PORT_CONFIG = "hbase.zookeeper.property.clientPort";
    /** Timeout in case, SEP is taking more time to consume the events. After timeout, RS will push the same load again**/
    private static final String RPC_TIMEOUT_CONFIG = "hbase.rpc.timeout";
	
	/** The localhost */
	private static final String LOCAL_HOST_NAME = "localhost";
	
	/** The default ZK settings*/
	private static final int ZK_CLIENT_PORT = 2181;
	private static final int ZK_SESSION_TIMEOUT = 20000; // 20 seconds
    private static final String RPC_TIMEOUT = "60000"; //60 sec default
	
	/** The default number of worker threads that process WAL edit events*/
	private static final int WORKER_THREADS = 1;

    /**RPC Timeout for SEP to consume the events sent by RS**/
    protected String rpcTimeout = RPC_TIMEOUT;

	/** The SEP consumer instance initialized by this Producer*/
	protected SepConsumer sepConsumer;
    /** The SepEventMapper for translating WAL edits to change events*/
    protected SepEventMapper<T> sepEventMapper;

	/** The Zookeeper connection properties*/
	protected String zkQuorum;
	protected int zkClientPort = ZK_CLIENT_PORT;
	protected Integer zkSessionTimeout = ZK_SESSION_TIMEOUT;
	
	/** The number of WAL edits processing worker threads*/
	protected int workerThreads = WORKER_THREADS;

    /** The ScnGenerator for generating relayer Scn*/
    protected SCNGenerator scnGenerator = new MonotonicSequenceSCNGenerator();

    private volatile AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    /** Host name where this producer is running i.e. local host name*/
    private String localHost;

	/**
	 * Interface method implementation. Checks for mandatory dependencies and creates the SEP consumer
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.zkQuorum,"'zkQuorum' cannot be null. Zookeeper quorum list must be specified. This HBase Events producer will not be initialized");
        if (this.zkQuorum.contains(":")) {
            throw new IllegalStateException("'zkQuorum' is comma separated list of only hosts. Specify port using 'zkClientPort' : " + this.zkQuorum);
        }		
		Assert.notNull(this.sepEventMapper,"'sepEventMapper' cannot be null. No WAL edits event mapper found. This HBase Events producer will not be initialized");
		if (this.zkQuorum.contains(LOCAL_HOST_NAME)) { // we dont want 'localhost' to resolve to other names - say from /etc/hosts if ZK is also running locally during testing
			this.localHost = LOCAL_HOST_NAME;
		} else {
			this.localHost = InetAddress.getLocalHost().getHostName();
		}
	}
	
	/**
	 * Interface method implementation. Starts up the SEP consumer
	 * @see com.linkedin.databus2.producers.EventProducer#start(long)
	 */
	public void start (long sinceSCN) {
		shutdownRequested.set(false);
        this.sinceSCN.set(sinceSCN);
		LOGGER.info("Starting SEP subscription : " + this.getName());
		LOGGER.info("ZK quorum hosts : " + this.zkQuorum);
		LOGGER.info("ZK client port : " + this.zkClientPort);
        LOGGER.info("ZK session timeout : " + this.zkSessionTimeout);
        LOGGER.info("RPC timeout : " + this.rpcTimeout);
		LOGGER.info("Using hostname to bind to : " + this.localHost);
		LOGGER.info("Using worker threads : " + this.workerThreads);
		LOGGER.info("Listening to WAL edits from : " + this.sinceSCN);
		try {
	        Configuration hbaseConf = HBaseConfiguration.create();
	        // enable replication to get WAL edits
	        hbaseConf.setBoolean(HBASE_REPLICATION_CONFIG, true);
	        // need to explicitly set the ZK host and port details - hosts separated from port - see SepModelImpl constructor source code
	        hbaseConf.set(ZK_QUORUM_CONFIG, this.zkQuorum);
	        hbaseConf.setInt(ZK_CLIENT_PORT_CONFIG, this.zkClientPort);
            hbaseConf.set(RPC_TIMEOUT_CONFIG,this.rpcTimeout);

	        StringBuilder zkQuorumWithPort = new StringBuilder();
	        String[] zkHostsList = this.zkQuorum.split(",");
	        for (String zkHost : zkHostsList) {
	        	zkQuorumWithPort.append(zkHost);
	        	zkQuorumWithPort.append(":");
	        	zkQuorumWithPort.append(this.zkClientPort);
	        	zkQuorumWithPort.append(",");
	        }
	        
	        LOGGER.info("ZK util connect string (host:port) : " + zkQuorumWithPort.toString());
	        ZooKeeperItf zk = ZkUtil.connect(zkQuorumWithPort.toString(), this.zkSessionTimeout);
	        
	        StringBuilder hbaseConfBuilder = new StringBuilder();
	        Iterator<Entry<String, String>> it = hbaseConf.iterator();
	        while (it.hasNext()) {
	        	Entry<String,String> entry = it.next();
	        	if (entry.getKey().equalsIgnoreCase(HBASE_REPLICATION_CONFIG) || 
	        			entry.getKey().equalsIgnoreCase(ZK_QUORUM_CONFIG) ||
	        			entry.getKey().equalsIgnoreCase(ZK_CLIENT_PORT_CONFIG)) {
	        		hbaseConfBuilder.append(entry.getKey());
	        		hbaseConfBuilder.append(":");
	        		hbaseConfBuilder.append(entry.getValue());
	        		hbaseConfBuilder.append(",");
	        	}
	        }
	        LOGGER.info("SEP Model Hbase configuration = " + hbaseConfBuilder.toString());
	        SepModel sepModel = new SepModelImpl(zk, hbaseConf);

	        final String subscriptionName = this.getName();

	        if (!sepModel.hasSubscription(subscriptionName)) {
	            sepModel.addSubscriptionSilent(subscriptionName);
	        }
            this.sepConsumer = new SepConsumer(subscriptionName, generateSEPSCN(this.sinceSCN.get()), new RelayAppender(), this.workerThreads, this.localHost, zk, hbaseConf);
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
			if(shutdownRequested.get()){
				return;
			}
			long lastSavedSCN = sinceSCN.get();
			eventBuffer.startEvents();
            for (SepEvent sepEvent : sepEvents) {
            	T changeEvent = sepEventMapper.mapSepEvent(sepEvent);
            	byte[] schemaId=SchemaHelper.getSchemaId(changeEvent.getSchema().toString());
               	byte[] serializedEvent = serializeEvent(changeEvent);
            	// we find the earliest processed timestamp of the KVs available in the WAL Edit, so as to not miss any edits
            	long earliestKVTimestamp = Long.MAX_VALUE;
            	for (KeyValue kv : sepEvent.getKeyValues()) {
            		earliestKVTimestamp = Math.min(earliestKVTimestamp, kv.getTimestamp());
            	}
				DbusEventKey eventKey = new DbusEventKey(sepEvent.getRow()); // we use the SepEvent row key as the identifier
                long eventScnNumber = scnGenerator.getSCN(earliestKVTimestamp,localHost);
                DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,eventScnNumber,
                        (short)physicalSourceStaticConfig.getId(),(short)physicalSourceStaticConfig.getId(),
                        System.nanoTime(),(short)physicalSourceStaticConfig.getSources()[0].getId(), // here we use the Logical Source Id
                        schemaId,serializedEvent, false, true);
                eventBuffer.appendEvent(eventKey, eventInfo, dbusEventsStatisticsCollector);
                sinceSCN.set(Math.max(lastSavedSCN, eventScnNumber));
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
     * Utility to generate SEP SCN (WALEdits) from Relayer Scn.
     * @param relayerSCN - lastSavedSCN on the relayer. lastSavedSCN is (SepSCN<<10|running_number)
     * @return sepSCN
     */
    private long generateSEPSCN(long relayerSCN) {
        if (relayerSCN == -1 || relayerSCN == 0) {
            return relayerSCN;
        }
        return relayerSCN >> 10;
    }

	/**
	 * Interface method implementation. Stops the SEP consumer
	 * @see com.linkedin.databus2.producers.EventProducer#shutdown()
	 */
	public void shutdown() {
		LOGGER.info("Shutdown has been requested. HBaseEventProducer shutting down");
		this.shutdownRequested.set(true);
		this.sepConsumer.stop();
		super.shutdown();
		LOGGER.info("HBaseEventProducer shut down complete");
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
	public Integer getZkClientPort() {
		return zkClientPort;
	}
	public void setZkClientPort(int zkClientPort) {
		this.zkClientPort = zkClientPort;
	}
	public Integer getZkSessionTimeout() {
		return zkSessionTimeout;
	}
	public void setZkSessionTimeout(Integer zkSessionTimeout) {
		this.zkSessionTimeout = zkSessionTimeout;
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
    public SCNGenerator getScnGenerator() {
        return scnGenerator;
    }
    public void setScnGenerator(SCNGenerator scnGenerator) {
        this.scnGenerator = scnGenerator;
    }
    public String getRpcTimeout() {
        return rpcTimeout;
    }
    public void setRpcTimeout(String rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
    }
    /** End Setter/Getter methods*/

}
