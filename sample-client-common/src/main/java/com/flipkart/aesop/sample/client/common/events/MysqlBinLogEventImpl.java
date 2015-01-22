package com.flipkart.aesop.sample.client.common.events;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

import com.flipkart.aesop.sample.client.common.utils.AvroToMysqlMapper;
import com.flipkart.aesop.sample.client.common.utils.MysqlDataTypes;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * @author yogesh.dahiya
 */

public class MysqlBinLogEventImpl implements MysqlBinLogEvent
{
	private static String PK_FIELD_NAME = "pk";
	private static String META_FIELD_TYPE_NAME = "dbFieldType";
	private Schema schema;
	private List<String> pKeyList = new ArrayList<String>(3);
	private Map<String, Object> keyValuePairs = new HashMap<String, Object>();
	private DbusOpcode eventType;

	public MysqlBinLogEventImpl(DbusEvent event, DbusEventDecoder eventDecoder) throws DatabusException
	{
		GenericRecord genericRecord = eventDecoder.getGenericRecord(event, null);
		VersionedSchema writerSchema = eventDecoder.getPayloadSchema(event);
		this.schema = writerSchema.getSchema();
		this.eventType = event.getOpcode();
		this.pKeyList = getPkListFromSchema(schema);
		for (Field field : schema.getFields())
		{
			String mysqlType = SchemaHelper.getMetaField(field, META_FIELD_TYPE_NAME);
			this.keyValuePairs.put(
			        field.name(),
			        AvroToMysqlMapper.avroToMysqlType(genericRecord.get(field.name()),
			                MysqlDataTypes.valueOf(mysqlType.toUpperCase())));
		}

	}

	public MysqlBinLogEventImpl(Schema schema, Map<String, Object> keyValuePairs, DbusOpcode eventType)
	        throws DatabusException
	{
		this.eventType = eventType;
		this.keyValuePairs = keyValuePairs;
		this.pKeyList = getPkListFromSchema(schema);
		this.schema = schema;

	}

	private List<String> getPkListFromSchema(Schema schema) throws DatabusException
	{
		List<String> pKeyList = new ArrayList<String>(3);
		String pkFieldName = SchemaHelper.getMetaField(schema, PK_FIELD_NAME);
		if (pkFieldName == null)
		{
			throw new DatabusException("No primary key specified in the schema");
		}
		for (String s : pkFieldName.split(DbusConstants.COMPOUND_KEY_SEPARATOR))
		{
			pKeyList.add(s.trim());
		}
		assert (pKeyList.size() >= 1);
		return pKeyList;
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

	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("MysqlBinLogEventImpl [");
		builder.append("entityName : " + getEntityName() + ", ");
		builder.append("eventType : " + getEventType().toString() + ", ");
		builder.append("pKeyList : " + getPrimaryKeyList().toString() + ", ");
		builder.append("keyValuePairs : " + getKeyValuePair().toString());
		return builder.toString();
	}

}
