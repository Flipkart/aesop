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

import com.flipkart.aesop.events.sample.person.Person;
import com.flipkart.aesop.serializer.model.UserInfo;
import com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper;
import com.netflix.zeno.diff.TypeDiff;
import com.netflix.zeno.diff.TypeDiff.ObjectDiffScore;
import com.netflix.zeno.genericobject.GenericObject;
import com.netflix.zeno.genericobject.GenericObject.Field;
import com.netflix.zeno.genericobject.GenericObjectSerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * The <code>PersonDiffChangeEventMapper</code> is an implementation of the {@link DiffChangeEventMapper} that maps {@link com.flipkart.aesop.serializer.model.UserInfo}
 * data from the state engine to {@link Person} change event instances
 * 
 * @author Regunath B
 * @version 1.0, 20 March 2014
 */
public class PersonDiffChangeEventMapper implements DiffChangeEventMapper<UserInfo,Person> {

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
	 * Interface method implementation. Returns {@link Person#getKey()}
	 * @see com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper#getChangeEventKey(org.apache.avro.generic.GenericRecord)
	 */
	public Object getChangeEventKey(Person person) {
		return person.getKey(); // here we are overloading the Person to also act as a change event object for testing
	}

	/**
	 * Interface method implementation. Returns {@link System#currentTimeMillis()} for testing
	 * @see com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper#getSequenceId(org.apache.avro.generic.GenericRecord)
	 */
	public Long getSequenceId(Person changeEvent) {
		return System.currentTimeMillis();
	}
	
	/**
	 * Interface method implementation. Returns a List of {@link Person} instances derived from data contained in the specified {@link TypeDiff} containing
	 * diff information on {@link UserInfo} objects
	 * @see com.flipkart.aesop.serializer.stateengine.DiffChangeEventMapper#getChangeEvents(com.netflix.zeno.diff.TypeDiff)
	 */
	public List<Person> getChangeEvents(TypeDiff<UserInfo> typeDiff, SerializerFactory serializerFactory) {
		List<Person> personsList = new LinkedList<Person>();
		GenericObjectSerializationFramework genericObjectFramework = new GenericObjectSerializationFramework(serializerFactory);
		for(ObjectDiffScore<UserInfo> objectDiff : typeDiff.getDiffObjects()) {
			GenericObject fromGenericObject = genericObjectFramework.serialize(objectDiff.getFrom(), this.getNFTypeName());
			GenericObject toGenericObject = genericObjectFramework.serialize(objectDiff.getTo(), this.getNFTypeName());
			String firstName = null;
			String lastName = null;
			boolean isDifferent = false;
			for(int i=0;i<fromGenericObject.getFields().size();i++) {
	            Field fromField = fromGenericObject.getFields().get(i);
	            Field toField = toGenericObject.getFields().get(i);	
	            // we are interested only in changes to first name and last name.
	            if (fromField.getFieldName().equals("first_name") || fromField.getFieldName().equals("last_name")) {
		            if (fromField.getValue() == null && toField.getValue() == null) {
		            	continue;
		            }
		            if ((fromField.getValue() == null && toField.getValue() != null) ||
		               (toField.getValue() == null && fromField.getValue() != null) ||
		               (!fromField.getValue().equals(toField.getValue()))) {
		            	isDifferent = true;
		            }
		            if (fromField.getFieldName().equals("first_name")) {
		            	firstName = fromField.getValue() + ":" + toField.getValue(); // concat original and changed name to one string - for testing
		            }
		            if (fromField.getFieldName().equals("last_name")) {
		            	lastName = fromField.getValue() + ":" + toField.getValue(); // concat original and changed name to one string - for testing		            	
		            }
	            }
			}
			if (isDifferent) {
				personsList.add(new Person(Long.valueOf(objectDiff.getFrom().getPrimary_phone()), firstName, lastName,0L,"false"));
			}
		}
		return personsList;
	}

}
