package com.flipkart.aesop.sample.client.common.events;

import java.util.*;

import com.flipkart.aesop.utils.AvroToMysqlConverter;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.flipkart.aesop.utils.AvroSchemaHelper;

/**
 * @author yogesh.dahiya
 */

public class MysqlBinLogEventImpl implements MysqlBinLogEvent
{
	private Schema schema;
	private List<String> pKeyList = new ArrayList<String>(3);
	private Map<String, Object> keyValuePairs = new HashMap<String, Object>();
	private Map<String, Object> rowChangeMap = new HashMap<String, Object>();
	private DbusOpcode eventType;

	public MysqlBinLogEventImpl(DbusEvent event, DbusEventDecoder eventDecoder) throws DatabusException
	{
		GenericRecord genericRecord = eventDecoder.getGenericRecord(event, null);
		VersionedSchema writerSchema = eventDecoder.getPayloadSchema(event);
		this.schema = writerSchema.getSchema();
		this.eventType = event.getOpcode();
		this.pKeyList = getPkListFromSchema(schema);
		this.rowChangeMap = null;
		Map <String, String> fieldToMysqlDataType = AvroSchemaHelper.fieldToDataTypeMap(schema);
		String rowChangeField = AvroSchemaHelper.getRowChangeField(schema);

		for (Field field : schema.getFields())
		{
			Object recordValue = genericRecord.get(field.name());
			if (field.name().equals(rowChangeField))
			{
				this.rowChangeMap = AvroToMysqlConverter.getMysqlTypedObjectForMap((Map<Object, Object>) recordValue,
						fieldToMysqlDataType);
			}
			else
			{
				this.keyValuePairs.put(field.name(),
						AvroToMysqlConverter.getMysqlTypedObject(fieldToMysqlDataType.get(field.name()), recordValue));
			}
		}

	}

	public MysqlBinLogEventImpl(Schema schema, Map<String, Object> keyValuePairs, DbusOpcode eventType, Map<String, Object> rowChangeMap)
	        throws DatabusException
	{
		this.eventType = eventType;
		this.keyValuePairs = keyValuePairs;
		this.pKeyList = getPkListFromSchema(schema);
		this.schema = schema;
		this.rowChangeMap = rowChangeMap;

	}

	private List<String> getPkListFromSchema(Schema schema) throws DatabusException
	{
		Set<String> primaryKeySet = AvroSchemaHelper.getPrimaryKeysSetFromSchema(schema);
		List<String> primaryKeyList = new ArrayList<String>(3);
		for (String s : primaryKeySet)
		{
			primaryKeyList.add(s);
		}
		return primaryKeyList;
	}

	public Map<String, Object> getKeyValuePair()
	{
		return this.keyValuePairs;
	}

	public Object get(String key)
	{
		return keyValuePairs.get(key);
	}

	public boolean isCompositeKey()
	{
		return pKeyList.size() > 1;
	}

	public List<String> getPrimaryKeyList()
	{
		return pKeyList;
	}

	public List<Object> getPrimaryKeyValues()
	{
		List<Object> primaryKeyValues = new ArrayList<Object>();
		for (int i = 0; i < pKeyList.size(); i++)
		{
			primaryKeyValues.add(keyValuePairs.get(pKeyList.get(i)));
		}
		return primaryKeyValues;
	}

	public String getEntityName()
	{
		return this.schema.getNamespace() + "." + this.schema.getName();
	}

	public DbusOpcode getEventType()
	{
		return this.eventType;
	}

	public Map<String, Object> getRowChangeMap() { return rowChangeMap; }

	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("MysqlBinLogEventImpl [");
		builder.append("entityName : " + getEntityName() + ", ");
		builder.append("eventType : " + getEventType().toString() + ", ");
		builder.append("pKeyList : " + getPrimaryKeyList().toString() + ", ");
		builder.append("keyValuePairs : " + getKeyValuePair().toString() + ", ");
		builder.append("rowChangeMap : " + getRowChangeMap());
		return builder.toString();
	}
}
