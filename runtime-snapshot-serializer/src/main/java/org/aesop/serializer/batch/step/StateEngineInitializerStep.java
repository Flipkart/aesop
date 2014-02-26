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

import java.io.File;

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

/**
 * The <code>StateEngineInitializerStep</code> class is an implementation of the Spring batch {@link Tasklet} that initializes the Zeno {@link FastBlobStateEngine}
 * for snapshot (as described here : {@link https://github.com/Netflix/zeno/wiki/Producing-a-Snapshot}) or a delta (as described here : https://github.com/Netflix/zeno/wiki/Producing-a-Delta).
 * 
 * This tasklet is intended to be used in a batch job definition comprising steps as described below:
 * <pre>
 * 	<step1>Use this tasklet to prepare Fast blob state engine for snapshot or delta write</step1>  
 * 	<step2>Fetch, process and append events to the Fast blob state engine</step2>
 * 	<step3>Write out the snapshot or delta to persistent store</step3>
 * </pre>
 * 
 * @author Regunath B
 * @version 1.0, 24 Feb 2014
 */

public class StateEngineInitializerStep implements Tasklet, InitializingBean {

	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(StateEngineInitializerStep.class);
	
	/** The FastBlobStateEngine to initialize*/
	private FastBlobStateEngine stateEngine;
	
	/** The file location for storing snapshots and deltas */
	private String serializedDataLocation;

	/**
	 * Interface method implementation. Initializes the Zeno {@link FastBlobStateEngine}
	 * @see org.springframework.batch.core.step.tasklet.Tasklet#execute(org.springframework.batch.core.StepContribution, org.springframework.batch.core.scope.context.ChunkContext)
	 */
	public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		if (!serializedDataLocationFile.exists()) {
			serializedDataLocationFile.mkdirs();
		}
		File snapshotsLocationFile = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		File deltaLocationFile = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);
		if (!snapshotsLocationFile.exists()) {
			snapshotsLocationFile.mkdirs();			
		}
		if (snapshotsLocationFile.listFiles().length == 0) {
			LOGGER.info("Fast blob state engine initialized for producing snapshot");
			return RepeatStatus.FINISHED; // snapshots do not exist, so return right away			
		}
		if (!deltaLocationFile.exists()) {
			deltaLocationFile.mkdirs();
		}
		// note that the decision to snapshot or produce delta is not based on foolproof logic. Ideally actual snapshot contents must be verified
		this.stateEngine.prepareForNextCycle(); // snapshot exists (at least the directory does), therefore initialize for delta
		LOGGER.info("Fast blob state engine initialized for producing delta");
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
