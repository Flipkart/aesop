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

import org.apache.commons.lang.math.NumberUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.core.serializer.support.SerializationFailedException;

import com.flipkart.aesop.events.sample.person.FieldChange;
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

	/** The ObjectMapper to use for JSON serialization of {@link UserInfo#getPreferences()}, {@link UserInfo#getAddresses()}, {@link UserInfo#getMerged_account_ids()} */
	private ObjectMapper objectMapper = new ObjectMapper();
	
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
			List<FieldChange> fieldChanges = new LinkedList<FieldChange>();
			for(int i=0;i<fromGenericObject.getFields().size();i++) {
	            Field fromField = fromGenericObject.getFields().get(i);
	            Field toField = toGenericObject.getFields().get(i);	
	            if (fromField.getValue() == null && toField.getValue() == null) {
	            	continue;
	            }
	            try {
		            if ((fromField.getValue() == null && toField.getValue() != null) ||
		               (toField.getValue() == null && fromField.getValue() != null) ||
		               (!fromField.getValue().equals(toField.getValue()))) {
		            	// serialize data as JSON if field names are : preferences, addresses or merged_account_ids
		            	String fieldFromValue = null;
		            	String fieldToValue = null;
		            	if (fromField.getFieldName().equals("preferences")) { 
		            		if (fromField.getValue() != null && toField.getValue() != null) { 
			            		fieldFromValue = objectMapper.writer().writeValueAsString(objectDiff.getFrom().getPreferences());
			            		fieldToValue = objectMapper.writer().writeValueAsString(objectDiff.getTo().getPreferences());
			            		if (fieldFromValue.equals(fieldToValue)) {
			            			continue;
			            		}
		            		} else {
			            		fieldFromValue = fromField.getValue() == null ? "null" : objectMapper.writer().writeValueAsString(objectDiff.getFrom().getPreferences());
			            		fieldToValue = toField.getValue() == null ? "null" :  objectMapper.writer().writeValueAsString(objectDiff.getTo().getPreferences());
		            		}
		            	} else if (fromField.getFieldName().equals("addresses")) {
		            		if (fromField.getValue() != null && toField.getValue() != null) {
			            		fieldFromValue = objectMapper.writer().writeValueAsString(objectDiff.getFrom().getAddresses());
			            		fieldToValue = objectMapper.writer().writeValueAsString(objectDiff.getTo().getAddresses());		            			
			            		if (fieldFromValue.equals(fieldToValue)) {
			            			continue;
			            		}
		            		} else {
			            		fieldFromValue = fromField.getValue() == null ? "null" : objectMapper.writer().writeValueAsString(objectDiff.getFrom().getAddresses());
			            		fieldToValue = toField.getValue() == null ? "null" :  objectMapper.writer().writeValueAsString(objectDiff.getTo().getAddresses());
		            		}
		            	} else if (fromField.getFieldName().equals("merged_account_ids")) {
		            		if (fromField.getValue() != null && toField.getValue() != null) { 
			            		fieldFromValue = objectMapper.writer().writeValueAsString(objectDiff.getFrom().getMerged_account_ids());
			            		fieldToValue = objectMapper.writer().writeValueAsString(objectDiff.getTo().getMerged_account_ids());		            			
			            		if (fieldFromValue.equals(fieldToValue)) {
			            			continue;
			            		}
		            		} else {
			            		fieldFromValue = fromField.getValue() == null ? "null" : objectMapper.writer().writeValueAsString(objectDiff.getFrom().getMerged_account_ids());
			            		fieldToValue = toField.getValue() == null ? "null" :  objectMapper.writer().writeValueAsString(objectDiff.getTo().getMerged_account_ids());
		            		}
		            	} else {
		            		fieldFromValue = fromField.getValue() == null ? "null" : fromField.getValue().toString();
		            		fieldToValue = toField.getValue() == null ? "null" : toField.getValue().toString();
		            	}
		            	fieldChanges.add(new FieldChange(fromField.getFieldName(), fieldFromValue, fieldToValue));
		            }
	            } catch (Exception e) {
	            	throw new SerializationFailedException("Error interpreting change from diff : " + e.getMessage(), e);
	            }
			}
			personsList.add(new Person(Long.valueOf(!NumberUtils.isNumber(objectDiff.getFrom().getPrimary_phone()) ? "0" : objectDiff.getFrom().getPrimary_phone().trim()), 
					objectDiff.getFrom().getFirst_name(), objectDiff.getFrom().getLast_name(),0L,"false",fieldChanges));
		}
		return personsList;
	}

}
