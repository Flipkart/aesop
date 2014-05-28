package com.flipkart.aesop.events.ortest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * yogesh.dahiya
 */

public class DefaultBinLogEvent
{
	private Schema schema;
	private List<String> pKeyList = new ArrayList<String>(3);
	private Map<String, Object> keyValuePairs = new HashMap<String, Object>();

	public DefaultBinLogEvent(DbusEvent event, DbusEventDecoder eventDecoder) throws DatabusException
	{
		GenericRecord genericRecord = eventDecoder.getGenericRecord(event, null);
		VersionedSchema writerSchema = eventDecoder.getPayloadSchema(event);
		this.schema = writerSchema.getSchema();
		String pkFieldName = SchemaHelper.getMetaField(schema, "pk");
		if (pkFieldName == null)
		{
			throw new DatabusException("No primary key specified in the schema");
		}
		String[] pKeyList = pkFieldName.split(DbusConstants.COMPOUND_KEY_SEPARATOR);
		assert (pKeyList.length >= 1);
		for (String s : pKeyList)
		{
			this.pKeyList.add(s.trim());
		}

		for (Field field : schema.getFields())
		{
			this.keyValuePairs.put(field.name(), genericRecord.get(field.name()));
		}
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

	public int getNumKeys()
	{
		return pKeyList.size();
	}

}
