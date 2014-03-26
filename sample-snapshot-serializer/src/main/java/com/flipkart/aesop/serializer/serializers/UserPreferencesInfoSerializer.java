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
package com.flipkart.aesop.serializer.serializers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.core.serializer.support.SerializationFailedException;

import com.flipkart.aesop.serializer.model.UserPreferencesInfo;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.MapSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

/**
 * The <code>UserPreferencesInfoSerializer</code> class is a sub-type of {@link NFTypeSerializer} for the root example model type {@link UserPreferencesInfo}
 * 
 * @author Regunath B
 * @version 1.0, 28 Feb 2014
 */
public class UserPreferencesInfoSerializer extends NFTypeSerializer<UserPreferencesInfo> {

	/** The ObjectMapper to use for JSON (de)serialization of {@link UserPreferencesInfo#getValue()}*/
	private ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Constructor for this class
	 */
	public UserPreferencesInfoSerializer() {
		super(UserPreferencesInfo.class.getName());
	}

	/**
	 * Creates a schema describing the type serialized by this serializer
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#createSchema()
	 */
	protected FastBlobSchema createSchema() {
		return schema(
			field("id",FieldType.STRING),
			field("preferences_name",FieldType.STRING),
			field("value", "MapOfValue"),
			field("version",FieldType.INT),
			field("last_modified",FieldType.STRING)
		);
	}

	/**
	 * Deserializes a UserPreferencesInfo instance from the specified NFDeserializationRecord
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#doDeserialize(com.netflix.zeno.serializer.NFDeserializationRecord)
	 */
	protected UserPreferencesInfo doDeserialize(NFDeserializationRecord record) {
		String id = deserializePrimitiveString(record, "id");
	    String preferences_name = deserializePrimitiveString(record, "preferences_name");
	    Map<String,String> valueAsStringMap = deserializeObject(record, "value");
	    Map<String,Object> value = new HashMap<String, Object>();
		Iterator<String> iterator = valueAsStringMap.keySet().iterator();
		while(iterator.hasNext()) {
			String key = iterator.next();
			try {
				value.put(key, objectMapper.readValue(valueAsStringMap.get(key), Object.class));
			} catch (Exception e) {
				throw new SerializationFailedException("Serialization failed for userPreferencesInfo.getValue().get(key). Error is : " + e.getMessage(), e);
			}				
		}			    
	    int version = deserializeInteger(record,"version");
	    String last_modified = deserializePrimitiveString(record, "last_modified");
		return new UserPreferencesInfo(id, preferences_name,value,version, last_modified);
	}

	/**
	 * Serializes the specified UserPreferencesInfo object into the specified NFSerializationRecord
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#doSerialize(java.lang.Object, com.netflix.zeno.serializer.NFSerializationRecord)
	 */
	public void doSerialize(UserPreferencesInfo userPreferencesInfo, NFSerializationRecord record) {
		serializePrimitive(record, "id", userPreferencesInfo.getId());		
		serializePrimitive(record, "preferences_name", userPreferencesInfo.getPreferences_name());	
		if (userPreferencesInfo.getValue() != null) {
			Map<String,String> valueAsStringMap = new HashMap<String, String>();	
			Iterator<String> iterator = userPreferencesInfo.getValue().keySet().iterator();
			while(iterator.hasNext()) {
				String key = iterator.next();
				try {
					valueAsStringMap.put(key, objectMapper.writer().writeValueAsString(userPreferencesInfo.getValue().get(key)));
				} catch (Exception e) {
					throw new SerializationFailedException("Serialization failed for userPreferencesInfo.getValue().get(key). Error is : " + e.getMessage(), e);
				}				
			}		
			serializeObject(record, "value", valueAsStringMap);	    
		}
		serializePrimitive(record, "version", userPreferencesInfo.getVersion());		
		serializePrimitive(record, "last_modified", userPreferencesInfo.getLast_modified());		
	}

	/**
	 * Returns a Collection of serializers required for referenced types
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#requiredSubSerializers()
	 */
	public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
		return serializers(
				new MapSerializer<String,String>("MapOfValue", new StringSerializer(), new StringSerializer())
		);
	}

}
