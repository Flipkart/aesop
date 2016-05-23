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
import java.util.Map;
import java.util.Set;

import com.flipkart.aesop.utils.AvroSchemaHelper;
import com.flipkart.aesop.utils.AvroToMysqlConverter;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.VersionedSchema;

/**
 * Abstract Event Factory to be extended by the various types of Event Factory Classes.
 * @author Prakhar Jain
 * @param <T> Actual Event Class
 */
public abstract class AbstractEventFactory<T extends AbstractEvent> implements EventFactory
{
	public AbstractEvent createEvent(DbusEvent dbusEvent, DbusEventDecoder eventDecoder) throws DatabusException
	{
		GenericRecord genericRecord = eventDecoder.getGenericRecord(dbusEvent, null);
		VersionedSchema writerSchema = eventDecoder.getPayloadSchema(dbusEvent);
		Schema schema = writerSchema.getSchema();
		DbusOpcode eventType = dbusEvent.getOpcode();
		Set<String> primaryKeysSet = AvroSchemaHelper.getPrimaryKeysSetFromSchema(schema);
		String namespaceName = schema.getNamespace();
		String entityName = schema.getName();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map <String, String> fieldToMysqlDataType = AvroSchemaHelper.fieldToDataTypeMap(schema);
		String rowChangeField = AvroSchemaHelper.getRowChangeField(schema);
		Map<String, Object> rowChangeMap = null;

		for (Field field : schema.getFields())
		{
			Object recordValue = genericRecord.get(field.name());
			if (field.name().equals(rowChangeField))
			{
				rowChangeMap = AvroToMysqlConverter.getMysqlTypedObjectForMap((Map<Object, Object>) recordValue,
						fieldToMysqlDataType);
			}
			else
			{
				fieldMap.put(field.name(),
						AvroToMysqlConverter.getMysqlTypedObject(fieldToMysqlDataType.get(field.name()), recordValue));
			}
		}
		AbstractEvent event = createEventInstance(fieldMap, primaryKeysSet, entityName, namespaceName, eventType,
				rowChangeMap);
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
														 String entityName, String namespaceName, DbusOpcode eventType, Map<String, Object> rowChangeMap);

	public AbstractEvent createEvent(Schema schema, Map<String, Object> keyValuePairs, DbusOpcode eventType, Map<String, Object> rowChangeMap)
			throws DatabusException
	{
		String entityName = schema.getName();
		String namespaceName = schema.getNamespace();
		Set<String> primaryKeysSet = AvroSchemaHelper.getPrimaryKeysSetFromSchema(schema);

		AbstractEvent event =
				createEventInstance(keyValuePairs, primaryKeysSet, entityName, namespaceName, eventType, rowChangeMap);
		return event;
	}

	public AbstractEvent createEvent(Map<String, Object> fieldsMap, Set<String> primaryFieldsSet, String entityName,
									 String namespaceName, DbusOpcode eventType, Map<String, Object> rowChangeMap)
	{
		AbstractEvent event =
				createEventInstance(fieldsMap, primaryFieldsSet, entityName, namespaceName, eventType, rowChangeMap);

		return event;
	}
}
