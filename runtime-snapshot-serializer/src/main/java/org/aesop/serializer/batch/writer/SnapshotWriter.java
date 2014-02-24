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
package org.aesop.serializer.batch.writer;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.netflix.zeno.fastblob.FastBlobStateEngine;

/**
 * The <code>SnapshotWriter</code> class is a simple implementation of the Spring Batch {@link ItemWriter}. This item writer uses the Zeno library to append to either
 * a snapshot (as described here : {@link https://github.com/Netflix/zeno/wiki/Producing-a-Snapshot}) or a delta (as described here : https://github.com/Netflix/zeno/wiki/Producing-a-Delta).
 * This writer is intended to be used in a batch job definition comprising steps as described below:
 * <pre>
 * 	<step1>Prepare Fast blob state engine for snapshot or delta write</step1>  
 * 	<step2>Use this writer along with a reader. processor to fetch data from source and append to Fast blob state engine</step2>
 * 	<step3>Write out the snapshot or delta to persistent store</step3>
 * </pre>
 * @author Regunath B
 * @version 1.0, 24 Feb 2014
 */
public class SnapshotWriter<T> implements ItemWriter<T>, InitializingBean {

	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(SnapshotWriter.class);
	
	/** The FastBlobStateEngine to append the items to*/
	private FastBlobStateEngine stateEngine;
	
	/**
	 * Interface method implementation. Adds the items to the FastBlobStateEngine
	 * @see org.springframework.batch.item.ItemWriter#write(java.util.List)
	 */
	public void write(List<? extends T> items) throws Exception {
		for (T item : items) {
			this.stateEngine.add(item.getClass().getName(), item);
		}
		LOGGER.debug("Appended {} items of type {} to Fast blob state engine", items.size(), items.get(0).getClass().getName());
	}

	/**
	 * Interface method implementation. Checks for mandatory dependencies
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.stateEngine,"'stateEngine' cannot be null. This Snaphot writer will not be initialized");
	}
	
	/** Getter/Setter methods */
	public FastBlobStateEngine getStateEngine() {
		return stateEngine;
	}
	public void setStateEngine(FastBlobStateEngine stateEngine) {
		this.stateEngine = stateEngine;
	}

}
