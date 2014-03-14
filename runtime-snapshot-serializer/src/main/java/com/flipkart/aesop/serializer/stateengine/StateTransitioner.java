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

import java.io.File;

import org.aesop.serializer.SerializerConstants;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * The <code>StateTransitioner</code> interface creates or suitably initializes an existing {@link FastBlobStateEngine} for producing snapshots or deltas.
 * 
 * @author Regunath B
 * @version 1.0, 5 March 2014
 */

public abstract class StateTransitioner implements InitializingBean {
	
	/** The SerializerFactory used for creating the FastBlobStateEngine instance*/
	protected SerializerFactory serializerFactory;
	
	/** The file location for storing snapshots and deltas */
	protected String serializedDataLocation;
	
	/**
	 * Returns a newly created or suitably initialized FastBlobStateEngine 
	 * @return FastBlobStateEngine instance
	 */
	public abstract FastBlobStateEngine getStateEngine();
	
	/**
	 * Saves the state held by the FastBlobStateEngine
	 */
	public abstract void saveState();
	
	/**
	 * Interface method implementation. Checks for mandatory dependencies
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.serializerFactory,"'serializerFactory' cannot be null. This state transitioner will not be initialized");
		Assert.notNull(this.serializedDataLocation,"'serializedDataLocation' cannot be null. This state transitioner will not be initialized");
		this.initializeDirs();
	}

	/** Getter/Setter methods */
	public void setSerializerFactory(SerializerFactory serializerFactory) {
		this.serializerFactory = serializerFactory;
	}
	public void setSerializedDataLocation(String serializedDataLocation) {
		this.serializedDataLocation = serializedDataLocation;
	}			
	public String getSerializedDataLocation() {
		return serializedDataLocation;
	}

	/**
	 * Initializes the snapshot and delta file directories
	 */
	protected void initializeDirs() {
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		if (!serializedDataLocationFile.exists()) {
			serializedDataLocationFile.mkdirs();
		}
		File snapshotsLocationFile = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		File deltaLocationFile = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);
		if (!snapshotsLocationFile.exists()) {
			snapshotsLocationFile.mkdirs();			
		}
		if (!deltaLocationFile.exists()) {
			deltaLocationFile.mkdirs();
		}		
	}
	
}
