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
package org.aesop.runtime.producer.hbase;

import org.aesop.runtime.producer.AbstractEventProducer;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.impl.SepConsumer;

/**
 * <code>AbstractHBaseEventProducer</code> that listens to HBase WAL edits using the hbase-sep module library classes such as {@link SepConsumer} and {@link EventListener} and in 
 * turn creates change events of {@link GenericRecord} sub-type T.
 *
 * @author Regunath B
 * @version 1.0, 17 Jan 2014
 */

public abstract class AbstractHBaseEventProducer<T extends GenericRecord> extends AbstractEventProducer implements InitializingBean {

	/** The SEP consumer instance initialized by this Producer*/
	protected SepConsumer sepConsumer;
	
	/** The Zookeeper connection properties*/
	protected String zkQuorum;
	protected Integer zkPort;
	
	/**
	 * Interface method implementation. Checks for mandatory dependencies and creates the SEP consumer
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.zkQuorum,"'zkQuorum' cannot be null. Zookeeper quorum list must be specified. This HBase Events producer will not be initialized");
		Assert.notNull(this.zkPort,"'zkPort' cannot be null. Zookeeper port must be specified. This HBase Events producer will not be initialized");
	}
	
	/**
	 * Interface method implementation. Starts up the SEP consumer
	 * @see com.linkedin.databus2.producers.EventProducer#start(long)
	 */
	public void start (long sinceSCN) {
		this.sinceSCN.set(sinceSCN);
	}

	/**
	 * Interface method implementation.
	 * @see com.linkedin.databus2.producers.EventProducer#getSCN()
	 */
	public long getSCN() {
		return this.sinceSCN.get();
	}

	/**
	 * Interface method implementation. Returns false always
	 * @see com.linkedin.databus2.producers.EventProducer#isPaused()
	 */
	public boolean isPaused() {
		return false;
	}

	/**
	 * Interface method implementation. Returns true always
	 * @see com.linkedin.databus2.producers.EventProducer#isRunning()
	 */
	public boolean isRunning() {
		return true;
	}

	/** No Op methods*/
	public void pause() {}
	public void shutdown() {}
	public void unpause() {}
	public void waitForShutdown() throws InterruptedException,IllegalStateException {}
	public void waitForShutdown(long time) throws InterruptedException,IllegalStateException {}

	/** Setter/Getter methods*/
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
		
}
