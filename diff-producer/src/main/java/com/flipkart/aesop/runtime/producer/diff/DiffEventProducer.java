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
package com.flipkart.aesop.runtime.producer.diff;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.InitializingBean;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.AbstractEventProducer;
import com.netflix.zeno.fastblob.FastBlobStateEngine;

/**
 * <code>DiffEventProducer</code> is a sub-type of {@link AbstractEventProducer} that interprets change events by loading snapshots and deltas onto a 
 * Zeno {@link FastBlobStateEngine} instance and listening in on the state change.
 *
 * @author Regunath B
 * @version 1.0, 17 March 2014
 */
public class DiffEventProducer <T extends GenericRecord> extends AbstractEventProducer implements InitializingBean {

	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(DiffEventProducer.class);
	
	/**
	 * Interface method implementation. 
	 * @see com.linkedin.databus2.producers.EventProducer#getName()
	 */
	public String getName() {
		return null;
	}

	@Override
	public long getSCN() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isPaused() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void pause() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void start(long arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unpause() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void waitForShutdown() throws InterruptedException,
			IllegalStateException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void waitForShutdown(long arg0) throws InterruptedException,
			IllegalStateException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
