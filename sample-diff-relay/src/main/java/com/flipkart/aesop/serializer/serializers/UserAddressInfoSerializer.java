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

import com.flipkart.aesop.serializer.model.UserAddressInfo;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.MapSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

/**
 * The <code>UserAddressInfoSerializer</code> class is a sub-type of {@link NFTypeSerializer} for the root example model type {@link UserAddressInfo}
 * 
 * @author Regunath B
 * @version 1.0, 27 Feb 2014
 */
public class UserAddressInfoSerializer extends NFTypeSerializer<UserAddressInfo> {

	/** The ObjectMapper to use for JSON (de)serialization of {@link UserAddressInfo#getPreferences()}*/
	private ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Constructor for this class
	 */
	public UserAddressInfoSerializer() {
		super(UserAddressInfo.class.getName());		
	}

	/**
	 * Creates a schema describing the type serialized by this serializer
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#createSchema()
	 */
	protected FastBlobSchema createSchema() {
		return schema(
			field("id",FieldType.STRING),
			field("account_id",FieldType.STRING),
			field("first_name",FieldType.STRING),
			field("last_name",FieldType.STRING),
			field("address_line1",FieldType.STRING),
			field("address_line2",FieldType.STRING),
			field("landmark",FieldType.STRING),
			field("city",FieldType.STRING),
			field("state",FieldType.STRING),
			field("state_code",FieldType.STRING),
			field("country",FieldType.STRING),
			field("pincode",FieldType.STRING),
			field("phone",FieldType.STRING),
			field("guest",FieldType.BOOLEAN),
			field("active",FieldType.BOOLEAN),
			field("version",FieldType.INT),
			field("creation_date",FieldType.STRING),
			field("last_modified",FieldType.STRING),
			field("creating_system",FieldType.STRING),
			field("preferences", "MapOfPreferences")
		);
	}

	/**
	 * Deserializes a UserAddressInfo instance from the specified NFDeserializationRecord
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#doDeserialize(com.netflix.zeno.serializer.NFDeserializationRecord)
	 */
	protected UserAddressInfo doDeserialize(NFDeserializationRecord record) {
		String id = deserializePrimitiveString(record, "id");
	    String account_id = deserializePrimitiveString(record, "account_id"); 
	    String first_name = deserializePrimitiveString(record, "first_name");
	    String last_name = deserializePrimitiveString(record, "last_name");
	    String address_line1 = deserializePrimitiveString(record, "address_line1");
	    String address_line2 = deserializePrimitiveString(record, "address_line2");
	    String landmark = deserializePrimitiveString(record, "landmark");
	    String city = deserializePrimitiveString(record, "city");
	    String state = deserializePrimitiveString(record, "state");
	    String state_code = deserializePrimitiveString(record, "id");
	    String country = deserializePrimitiveString(record, "country");
	    String pincode = deserializePrimitiveString(record, "pincode");
	    String phone = deserializePrimitiveString(record, "phone");
	    boolean guest = deserializeBoolean(record, "guest");
	    boolean active = deserializeBoolean(record, "active");
	    int version = deserializeInteger(record, "version");
	    String creation_date = deserializePrimitiveString(record, "creation_date");
	    String last_modified = deserializePrimitiveString(record, "last_modified");
	    String creating_system = deserializePrimitiveString(record, "creating_system");
	    Map<String,Object> preferences = deserializeObject(record, "preferences");
		return new UserAddressInfo(id,account_id,first_name,last_name,  address_line1,  address_line2,landmark,city,state,state_code,
				 country,pincode,phone,guest,active,version,creation_date,last_modified,creating_system,preferences);
	}

	/**
	 * Serializes the specified UserAddressInfo object into the specified NFSerializationRecord
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#doSerialize(java.lang.Object, com.netflix.zeno.serializer.NFSerializationRecord)
	 */
	public void doSerialize(UserAddressInfo userAddressInfo, NFSerializationRecord record) {
		serializePrimitive(record, "id", userAddressInfo.getId());		
		serializePrimitive(record, "account_id", userAddressInfo.getAccount_id());		
		serializeObject(record, "preferences", userAddressInfo.getPreferences());	    
		serializePrimitive(record, "first_name", userAddressInfo.getFirst_name());		
		serializePrimitive(record, "last_name", userAddressInfo.getLast_name());		
		serializePrimitive(record, "address_line1", userAddressInfo.getAddress_line1());		
		serializePrimitive(record, "address_line2", userAddressInfo.getAddress_line2());		
		serializePrimitive(record, "landmark", userAddressInfo.getLandmark());		
		serializePrimitive(record, "city", userAddressInfo.getCity());		
		serializePrimitive(record, "state", userAddressInfo.getState());		
		serializePrimitive(record, "state_code", userAddressInfo.getState_code());		
		serializePrimitive(record, "country", userAddressInfo.getCountry());		
		serializePrimitive(record, "pincode", userAddressInfo.getPincode());		
		serializePrimitive(record, "phone", userAddressInfo.getPhone());		
		serializePrimitive(record, "guest", userAddressInfo.isGuest());		
		serializePrimitive(record, "active", userAddressInfo.isActive());		
		serializePrimitive(record, "version", userAddressInfo.getVersion());		
		serializePrimitive(record, "creation_date", userAddressInfo.getCreation_date());		
		serializePrimitive(record, "last_modified", userAddressInfo.getLast_modified());		
		serializePrimitive(record, "creating_system", userAddressInfo.getCreating_system());
		if (userAddressInfo.getPreferences() != null) {
			Map<String,String> preferencesAsStringMap = new HashMap<String, String>();	
			Iterator<String> iterator = userAddressInfo.getPreferences().keySet().iterator();
			while(iterator.hasNext()) {
				String key = iterator.next();
				try {
					preferencesAsStringMap.put(key, objectMapper.writer().writeValueAsString(userAddressInfo.getPreferences().get(key)));
				} catch (Exception e) {
					throw new SerializationFailedException("Serialization failed for userAddressInfo.getPreferences().get(key). Error is : " + e.getMessage(), e);
				}				
			}
			serializeObject(record, "preferences", preferencesAsStringMap);
		}
	}

	/**
	 * Returns a Collection of serializers required for referenced types
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#requiredSubSerializers()
	 */
	public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
		return serializers(
				new MapSerializer<String,String>("MapOfPreferences", new StringSerializer(), new StringSerializer())
		);
	}

}
