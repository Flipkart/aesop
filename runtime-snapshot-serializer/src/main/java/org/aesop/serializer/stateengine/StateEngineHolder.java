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
package org.aesop.serializer.stateengine;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.netflix.zeno.fastblob.FastBlobStateEngine;

/**
 * The <code>StateEngineHolder</code> class is a holder for a {@link FastBlobStateEngine} that has been suitably initialized to produce snapshots or deltas.
 * 
 * @author Regunath B
 * @version 1.0, 5 March 2014
 */
public class StateEngineHolder implements InitializingBean{

	/** The StateTransitioner instance */
	private StateTransitioner stateTransitioner;
	
	/**
	 * Returns the FastBlobStateEngine to use. Delegates the call to the {@link StateTransitioner} instance configured
	 * @return the FastBlobStateEngine instance
	 */
	public FastBlobStateEngine getStateEngine() {
		return this.stateTransitioner.getStateEngine();
	}
	
	/**
	 * Interface method implementation. Checks for mandatory dependencies
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.stateTransitioner,"'stateTransitioner' cannot be null. This StateEngineHolder will not be initialized");
	}

	/** Getter/Setter methods */
	public StateTransitioner getStateTransitioner() {
		return stateTransitioner;
	}
	public void setStateTransitioner(StateTransitioner stateTransitioner) {
		this.stateTransitioner = stateTransitioner;
	}
		
}
