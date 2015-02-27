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
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.flipkart.aesop.serializer.model.UserAddressInfo;
import com.flipkart.aesop.serializer.model.UserInfo;
import com.flipkart.aesop.serializer.model.UserPreferencesInfo;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.ListSerializer;
import com.netflix.zeno.serializer.common.MapSerializer;
import com.netflix.zeno.serializer.common.SetSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

/**
 * The <code>UserInfoSerializer</code> class is a sub-type of {@link NFTypeSerializer} for the root example model type {@link UserInfo}.
 *
 * @author Regunath B
 * @version 1.0, 27 Feb 2014
 */

public class UserInfoSerializer extends NFTypeSerializer<UserInfo> {

	/**
	 * Constructor for this class
	 */
	public UserInfoSerializer() {
		super(UserInfo.class.getName());
	}

	/**
	 * Creates a schema describing the type serialized by this serializer
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#createSchema()
	 */
	protected FastBlobSchema createSchema() {
		return schema(
				field("id",FieldType.STRING),
				field("primary_account_id",FieldType.STRING),
				field("first_name",FieldType.STRING),
				field("last_name",FieldType.STRING),
				field("primary_email",FieldType.STRING),
				field("primary_phone",FieldType.STRING),
				field("profile_name",FieldType.STRING),
				field("blacklisted_parent",FieldType.STRING),
				field("status",FieldType.STRING),
				field("active",FieldType.BOOLEAN),
				field("guest",FieldType.BOOLEAN),
				field("blacklisted",FieldType.BOOLEAN),
				field("preferences", "MapOfPreferences"),
				field("addresses", "SetOfAddresses"),
				field("merged_account_ids", "MergedAccountIdsList"),
				field("version",FieldType.INT),
				field("creation_date",FieldType.STRING),
				field("last_modified",FieldType.STRING),
				field("creating_system",FieldType.STRING)
		);
	}

	/**
	 * Deserializes a UserInfo instance from the specified NFDeserializationRecord
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#doDeserialize(com.netflix.zeno.serializer.NFDeserializationRecord)
	 */
	protected UserInfo doDeserialize(NFDeserializationRecord record) {
		String id = deserializePrimitiveString(record, "id");
	    String primary_account_id = deserializePrimitiveString(record, "primary_account_id");
	    String first_name = deserializePrimitiveString(record, "first_name");
	    String last_name = deserializePrimitiveString(record, "last_name");
	    String primary_email = deserializePrimitiveString(record, "primary_email");
	    String primary_phone = deserializePrimitiveString(record, "primary_phone");
	    String profile_name = deserializePrimitiveString(record, "profile_name");
	    String blacklisted_parent = deserializePrimitiveString(record, "blacklisted_parent");
	    String status = deserializePrimitiveString(record, "status");

	    boolean active = deserializeBoolean(record, "active");
	    boolean guest = deserializeBoolean(record, "guest");
	    boolean blacklisted = deserializeBoolean(record, "blacklisted");

	    Map<String,UserPreferencesInfo> preferences = deserializeObject(record, "preferences");
	    Set<UserAddressInfo> addresses = deserializeObject(record, "addresses");
	    List<String> merged_account_ids = deserializeObject(record, "merged_account_ids");

	    int version = deserializeInteger(record,"version");
	    String creation_date = deserializePrimitiveString(record, "creation_date");
	    String last_modified = deserializePrimitiveString(record, "last_modified");
	    String creating_system = deserializePrimitiveString(record, "creating_system");

		return new UserInfo(id,primary_account_id,first_name,last_name,primary_email,primary_phone,
				 profile_name,blacklisted_parent,status,active,guest,blacklisted,preferences,addresses,merged_account_ids,version,creation_date,last_modified,
				 creating_system);
	}

	/**
	 * Serializes the specified UserInfo object into the specified NFSerializationRecord
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#doSerialize(java.lang.Object, com.netflix.zeno.serializer.NFSerializationRecord)
	 */
	public void doSerialize(UserInfo userInfo, NFSerializationRecord record) {
		serializePrimitive(record, "id", userInfo.getId());
		serializePrimitive(record, "primary_account_id", userInfo.getPrimary_account_id());
		serializePrimitive(record, "first_name", userInfo.getFirst_name());
		serializePrimitive(record, "last_name", userInfo.getLast_name());
		serializePrimitive(record, "primary_email", userInfo.getPrimary_email());
		serializePrimitive(record, "primary_phone", userInfo.getPrimary_phone());
		serializePrimitive(record, "profile_name", userInfo.getProfile_name());
		serializePrimitive(record, "blacklisted_parent", userInfo.getBlacklisted_parent());
		serializePrimitive(record, "status", userInfo.getStatus());
		serializePrimitive(record, "active", userInfo.isActive());
		serializePrimitive(record, "guest", userInfo.isGuest());
		serializePrimitive(record, "blacklisted", userInfo.getBlacklisted());

		serializeObject(record, "preferences", userInfo.getPreferences());
		serializeObject(record, "addresses", userInfo.getAddresses());
		serializeObject(record, "merged_account_ids", userInfo.getMerged_account_ids());

		serializePrimitive(record, "version", userInfo.getVersion());
		serializePrimitive(record, "creation_date", userInfo.getCreation_date());
		serializePrimitive(record, "last_modified", userInfo.getLast_modified());
		serializePrimitive(record, "creating_system", userInfo.getCreating_system());

	}

	/**
	 * Returns a Collection of serializers required for referenced types
	 * @see com.netflix.zeno.serializer.NFTypeSerializer#requiredSubSerializers()
	 */
	public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
		return serializers(
			new MapSerializer<String,UserPreferencesInfo>("MapOfPreferences", new StringSerializer(), new UserPreferencesInfoSerializer()),
			new SetSerializer<UserAddressInfo>("SetOfAddresses", new UserAddressInfoSerializer()),
			new ListSerializer<String>("MergedAccountIdsList", new StringSerializer())
	     );
	}

}
