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
package org.aesop.serializer.serializers;

import java.util.Collection;
import java.util.Map;

import org.aesop.serializer.model.UserPreferencesInfo;

import com.netflix.zeno.fastblob.record.FastBlobSchema;
import com.netflix.zeno.fastblob.record.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.MapSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

/**
 * The <code>UserPreferencesInfoSerializer</code> class is a sub-type of {@link NFTypeSerializer} for the root example model type {@link UserPreferencesInfo}
 * 
 * @author Regunath B
 * @version 1.0, 29 Feb 2014
 */
public class UserPreferencesInfoSerializer extends NFTypeSerializer<UserPreferencesInfo> {

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
			field("value"),
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
	    Map<String,String> value = deserializeObject(record, "value", "value");
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
		serializeObject(record, "value", "value", userPreferencesInfo.getValue());	    
		serializePrimitive(record, "version", userPreferencesInfo.getVersion());		
		serializePrimitive(record, "last_modified", userPreferencesInfo.getLast_modified());		
	}

	/**
	 * Returns a Collection of serializers required for referenced types
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#requiredSubSerializers()
	 */
	public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
		return serializers(
				new MapSerializer<String,String>("value", new StringSerializer(), new StringSerializer())
		);
	}

}
