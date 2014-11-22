/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 *******************************************************************************/

package com.flipkart.aesop.event;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

import com.flipkart.aesop.utils.AvroToMysqlMapper;
import com.flipkart.aesop.utils.MysqlDataTypes;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * Abstract Event Factory to be extended by the various types of Event Factory Classes.
 * @author Prakhar Jain
 * @param <T> Actual Event Class
 */
public abstract class AbstractEventFactory<T extends AbstractEvent> implements EventFactory
{
	public static String PRIMARY_KEY_FIELD_NAME = "pk";
	public static String META_FIELD_TYPE_NAME = "dbFieldType";

	/**
	 * Generates primary key set using the schema.
	 * @param schema
	 * @return Primary key set
	 * @throws DatabusException
	 */
	private Set<String> getPrimaryKeysSetFromSchema(Schema schema) throws DatabusException
	{
		Set<String> primaryKeySet = new HashSet<String>();
		String primaryKeyFieldName = SchemaHelper.getMetaField(schema, PRIMARY_KEY_FIELD_NAME);
		if (primaryKeyFieldName == null)
		{
			throw new DatabusException("No primary key specified in the schema");
		}
		for (String primaryKey : primaryKeyFieldName.split(DbusConstants.COMPOUND_KEY_SEPARATOR))
		{
			primaryKeySet.add(primaryKey.trim());
		}
		assert (primaryKeySet.size() >= 1);
		return primaryKeySet;
	}

	public AbstractEvent createEvent(DbusEvent dbusEvent, DbusEventDecoder eventDecoder) throws DatabusException
	{
		GenericRecord genericRecord = eventDecoder.getGenericRecord(dbusEvent, null);
		VersionedSchema writerSchema = eventDecoder.getPayloadSchema(dbusEvent);
		Schema schema = writerSchema.getSchema();
		DbusOpcode eventType = dbusEvent.getOpcode();
		Set<String> primaryKeysSet = getPrimaryKeysSetFromSchema(schema);
		String namespaceName = schema.getNamespace();
		String entityName = schema.getName();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		for (Field field : schema.getFields())
		{
			String mysqlType = SchemaHelper.getMetaField(field, META_FIELD_TYPE_NAME);
			fieldMap.put(
			        field.name(),
			        AvroToMysqlMapper.avroToMysqlType(genericRecord.get(field.name()),
			                MysqlDataTypes.valueOf(mysqlType.toUpperCase())));
		}
		AbstractEvent event = createEventInstance(fieldMap, primaryKeysSet, entityName, namespaceName, eventType);
		return event;
	}

	/**
	 * To be implemented by the Actual Event class. Generates Event instance using all the mandatory fields.
	 * @param fieldsMap
	 * @param primaryKeysSet
	 * @param entityName
	 * @param namespaceName
	 * @param eventType
	 * @return Actual Event instance.
	 */
	protected abstract AbstractEvent createEventInstance(Map<String, Object> fieldsMap, Set<String> primaryKeysSet,
	        String entityName, String namespaceName, DbusOpcode eventType);

	public AbstractEvent createEvent(Schema schema, Map<String, Object> keyValuePairs, DbusOpcode eventType)
	        throws DatabusException
	{
		String entityName = schema.getName();
		String namespaceName = schema.getNamespace();
		Set<String> primaryKeysSet = getPrimaryKeysSetFromSchema(schema);

		AbstractEvent event = createEventInstance(keyValuePairs, primaryKeysSet, entityName, namespaceName, eventType);
		return event;
	}

	public AbstractEvent createEvent(Map<String, Object> fieldsMap, Set<String> primaryFieldsSet, String entityName,
	        String namespaceName, DbusOpcode eventType)
	{
		AbstractEvent event = createEventInstance(fieldsMap, primaryFieldsSet, entityName, namespaceName, eventType);

		return event;
	}
}
