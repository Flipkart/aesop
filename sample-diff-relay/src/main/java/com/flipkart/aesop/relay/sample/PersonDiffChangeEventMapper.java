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
package com.flipkart.aesop.relay.sample;

import java.util.LinkedList;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.aesop.events.sample.person.Person;
import com.flipkart.aesop.serializer.model.UserInfo;
import com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper;
import com.netflix.zeno.diff.TypeDiff;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * The <code>PersonDiffChangeEventMapper</code> is an implementation of the {@link DiffChangeEventMapper} that maps {@link com.flipkart.aesop.serializer.model.UserInfo}
 * data from the state engine to {@link Person} change event instances
 * 
 * @author Regunath B
 * @version 1.0, 20 March 2014
 */
public class PersonDiffChangeEventMapper implements DiffChangeEventMapper<UserInfo,Person>, InitializingBean {

	/** The SerializerFactory used for creating the FastBlobStateEngine instance*/
	private SerializerFactory serializerFactory;

	/**
	 * Interface method implementation. Checks for mandatory dependencies
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.serializerFactory,"'serializerFactory' cannot be null. This DiffChangeEventMapper will not be initialized");
	}
	
	/**
	 * Interface method implementation. Returns the type of {@link com.flipkart.aesop.serializer.model.UserInfo}
	 * @see com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper#getNFTypeName()
	 */
	public String getNFTypeName() {
		return UserInfo.class.getName();
	}

	/**
	 * Interface method implementation. Returns {@link UserInfo#getId()}
	 * @see com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper#getTypeKey(java.lang.Object)
	 */
	public Object getTypeKey(UserInfo userInfo) {
		return userInfo.getId();
	}

	/**
	 * Interface method implementation. Returns the SerializerFactory instance set on this DiffChangeEventMapper
	 * @see com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper#getSerializerFactory()
	 */
	public SerializerFactory getSerializerFactory() {
		return this.serializerFactory;
	}

	/**
	 * Interface method implementation. Returns a List of {@link Person} instances derived from data contained in the specified {@link TypeDiff} containing
	 * diff information on {@link UserInfo} objects
	 * @see com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper#getChangeEvents(com.netflix.zeno.diff.TypeDiff)
	 */
	public List<Person> getChangeEvents(TypeDiff<UserInfo> typeDiff) {
		List<Person> personsList = new LinkedList<Person>();
		return personsList;
	}

	/** Getter/Setter methods */
	public void setSerializerFactory(SerializerFactory serializerFactory) {
		this.serializerFactory = serializerFactory;
	}
	
}
