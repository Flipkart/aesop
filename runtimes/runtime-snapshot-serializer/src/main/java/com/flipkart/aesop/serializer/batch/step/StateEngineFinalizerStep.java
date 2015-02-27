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
package com.flipkart.aesop.serializer.batch.step;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.aesop.serializer.stateengine.StateTransitioner;
import com.netflix.zeno.fastblob.FastBlobStateEngine;

/**
 * The <code>StateEngineFinalizerStep</code> class is an implementation of the Spring batch {@link Tasklet} that writes contents of the Zeno {@link FastBlobStateEngine}
 * for snapshot (as described here : {@link https://github.com/Netflix/zeno/wiki/Producing-a-Snapshot}) or a delta (as described here : https://github.com/Netflix/zeno/wiki/Producing-a-Delta).
 * 
 * This tasklet is intended to be used in a batch job definition comprising steps as described below:
 * <pre>
 * 	<step1>Fetch, process and append events to the Fast blob state engine</step1>
 * 	<step2>Use this tasklet to write out the snapshot or delta to persistent store</step2>
 * </pre>
 * 
 * @author Regunath B
 * @version 1.0, 24 Feb 2014
 */

public class StateEngineFinalizerStep<T> implements Tasklet, InitializingBean {
	
	/** The StateTransitioner for accessing the FastBlobStateEngine used in appending items*/
	private StateTransitioner<T> stateTransitioner;
	
	/**
	 * Interface method implementation. Writes out contents of the Zeno {@link FastBlobStateEngine}
	 * @see org.springframework.batch.core.step.tasklet.Tasklet#execute(org.springframework.batch.core.StepContribution, org.springframework.batch.core.scope.context.ChunkContext)
	 */
	public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
		this.stateTransitioner.saveState();
		return RepeatStatus.FINISHED;		
	}
	
	/**
	 * Interface method implementation. Checks for mandatory dependencies 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.stateTransitioner,"'stateTransitioner' cannot be null. This batch step will not be initialized");
	}
	
	/** Getter/Setter methods */
	public StateTransitioner<T> getStateTransitioner() {
		return stateTransitioner;
	}
	public void setStateTransitioner(StateTransitioner<T> stateTransitioner) {
		this.stateTransitioner = stateTransitioner;
	}
	
}
