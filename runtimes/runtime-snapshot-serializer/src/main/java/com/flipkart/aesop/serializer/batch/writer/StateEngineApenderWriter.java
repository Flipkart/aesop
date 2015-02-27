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
package com.flipkart.aesop.serializer.batch.writer;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.serializer.stateengine.StateTransitioner;
import com.netflix.zeno.fastblob.FastBlobStateEngine;

/**
 * The <code>StateEngineApenderWriter</code> class is a simple implementation of the Spring Batch {@link ItemWriter}. This item writer appends the passed-in items
 * to the {@link FastBlobStateEngine} set on this writer.
 * This writer is intended to be used in a batch job definition comprising steps as described below:
 * <pre>
 * 	<step1>Use this writer along with a reader, processor to fetch data from source and append to Fast blob state engine</step1>
 * 	<step2>Write out the snapshot or delta to persistent store</step2>
 * </pre>
 * @author Regunath B
 * @version 1.0, 24 Feb 2014
 */
public class StateEngineApenderWriter<T> implements ItemWriter<T>, InitializingBean {

	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(StateEngineApenderWriter.class);
	
	/** The StateTransitioner for accessing the FastBlobStateEngine used in appending items*/
	private StateTransitioner<T> stateTransitioner;
	
	/**
	 * Interface method implementation. Adds the items to the FastBlobStateEngine
	 * @see org.springframework.batch.item.ItemWriter#write(java.util.List)
	 */
	public void write(List<? extends T> items) throws Exception {
		for (T item : items) {
			this.stateTransitioner.getStateEngine().add(item.getClass().getName(), item);
		}
		LOGGER.debug("Appended {} items of type {} to Fast blob state engine", items.size(), items.get(0).getClass().getName());
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.stateTransitioner,"'stateTransitioner' cannot be null. This State engine appender writer will not be initialized");
	}

	/** Getter/Setter methods */
	public StateTransitioner<T> getStateTransitioner() {
		return stateTransitioner;
	}
	public void setStateTransitioner(StateTransitioner<T> stateTransitioner) {
		this.stateTransitioner = stateTransitioner;
	}

}
