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
package com.flipkart.aesop.serializer.stateengine;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * The <code>DiffInterpreter</code> loads serialized data snapshots and deltas onto a {@link FastBlobStateEngine} and provides methods to listen-in on the 
 * change that the state engine goes through.
 * 
 * @author Regunath B
 * @version 1.0, 17 March 2014
 */

public class DiffInterpreter implements InitializingBean {
	
	/** The SerializerFactory used for creating the FastBlobStateEngine instance*/
	protected SerializerFactory serializerFactory;
	
	/** The file location for reading snapshots and deltas */
	protected String serializedDataLocation;

	/**
	 * Interface method implementation. Checks for mandatory dependencies
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.serializerFactory,"'serializerFactory' cannot be null. This diff interpreter will not be initialized");
		Assert.notNull(this.serializedDataLocation,"'serializedDataLocation' cannot be null. This diff interpreter will not be initialized");
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

}
