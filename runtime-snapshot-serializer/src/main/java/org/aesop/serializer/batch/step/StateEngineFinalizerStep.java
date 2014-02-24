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
package org.aesop.serializer.batch.step;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.aesop.serializer.SerializerConstants;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobWriter;

public class StateEngineFinalizerStep implements Tasklet, InitializingBean {

	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(StateEngineFinalizerStep.class);
	
	/** The FastBlobStateEngine to append the items to*/
	private FastBlobStateEngine stateEngine;
	
	/** The file location for storing snapshots and deltas */
	private String serializedDataLocation;
	
	/**
	 * Interface method implementation. Writes out contents of the Zeno {@link FastBlobStateEngine}
	 * @see org.springframework.batch.core.step.tasklet.Tasklet#execute(org.springframework.batch.core.StepContribution, org.springframework.batch.core.scope.context.ChunkContext)
	 */
	public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
			    
		this.stateEngine.prepareForWrite();
	    // Create a writer, which will be responsible for creating snapshot and/or delta blobs.
	    FastBlobWriter writer = new FastBlobWriter(this.stateEngine);		
		
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		File snapshotsLocationFile = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		File deltaLocationFile = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);
		
		DataOutputStream dataOS = null;		
		if (snapshotsLocationFile.listFiles().length == 0) {
			dataOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(snapshotsLocationFile)));
			writer.writeSnapshot(dataOS);
		} else {
			dataOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(deltaLocationFile)));
			writer.writeDelta(dataOS);
		}
	    dataOS.close();
		LOGGER.info("Fast blob state engine data written to snapshot/delta");
		return RepeatStatus.FINISHED;
	}
	
	/**
	 * Interface method implementation. Checks for mandatory dependencies 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.stateEngine,"'stateEngine' cannot be null. This batch step will not be initialized");
		Assert.notNull(this.serializedDataLocation,"'serializedDataLocation' cannot be null. This batch step will not be initialized");
	}
	
	/** Getter/Setter methods */
	public FastBlobStateEngine getStateEngine() {
		return stateEngine;
	}
	public void setStateEngine(FastBlobStateEngine stateEngine) {
		this.stateEngine = stateEngine;
	}
	public String getSerializedDataLocation() {
		return serializedDataLocation;
	}
	public void setSerializedDataLocation(String serializedDataLocation) {
		this.serializedDataLocation = serializedDataLocation;
	}
	
}
