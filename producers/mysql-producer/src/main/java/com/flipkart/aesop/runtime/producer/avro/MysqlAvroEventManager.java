/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.producer.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.mapper.BinLogEventMapper;
import com.flipkart.aesop.runtime.producer.mapper.impl.DefaultBinLogEventMapper;
import com.flipkart.aesop.runtime.producer.mapper.impl.ORToAvroMapper;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.KeyPair;
import com.linkedin.databus2.producers.ds.PrimaryKeySchema;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaId;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * <code>MysqlAvroEventManager</code> deals with avro events and provides avro specific functionalities
 * such as framing an avro record and appending avro events to event buffer.
 * 
 * <pre>
 * <ul> 
 * <li>Provides means to serialize avro events</li>
 * <li>Provides means to extract key for an event</li>
 * <li>Provides means to append events to event buffer</li>
 * </ul>
 * </pre>
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public class MysqlAvroEventManager<T extends GenericRecord>
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(MysqlAvroEventManager.class);

	/** logical source id */
	protected final int lSourceId;

	/** physical source id */
	protected final int pSourceId;

	/** constructor for this class */
	public MysqlAvroEventManager(int lSourceId, int pSourceId) throws DatabusException
	{
		this.lSourceId = lSourceId;
		this.pSourceId = pSourceId;
	}

	/**
	 * Creates databus events and appends to event buffer
	 * @param changeEntry db change entry
	 * @param eventBuffer event buffer to which events are appended
	 * @param enableTracing
	 * @param dbusEventsStatisticsCollector statistics collector
	 * @return length of the event data added
	 * @throws EventCreationException Thrown when event creation failed for a databus source
	 * @throws UnsupportedKeyException Thrown when the data type of the "key" field is not a supported type
	 * @throws DatabusException Generic Databus Exception
	 */
	public int createAndAppendEvent(DbChangeEntry changeEntry, DbusEventBufferAppendable eventBuffer,
	        boolean enableTracing, DbusEventsStatisticsCollector dbusEventsStatisticsCollector)
	        throws EventCreationException, UnsupportedKeyException, DatabusException
	{
		LOGGER.debug("Request received for create and append event for " + changeEntry);
		Object keyObj = obtainKey(changeEntry);
		/** Construct the Databus Event key, determine the key type and construct the key */
		DbusEventKey eventKey = new DbusEventKey(keyObj);

		/** Get the md5 for the schema */
		SchemaId schemaId = SchemaId.createWithMd5(changeEntry.getSchema());
		byte[] payload = serializeEvent(changeEntry.getRecord());
		DbusEventInfo eventInfo =
		        new DbusEventInfo(changeEntry.getOpCode(), changeEntry.getScn(), (short) pSourceId, (short) pSourceId,
		                changeEntry.getTimestampInNanos(), (short) lSourceId, schemaId.getByteArray(), payload,
		                enableTracing, false);
		boolean success = eventBuffer.appendEvent(eventKey, eventInfo, dbusEventsStatisticsCollector);
		LOGGER.debug("Successfully created and appended event for " + changeEntry);
		return success ? payload.length : -1;
	}

	/**
	 * Used to frame avro record from the bin log change event
	 * @param eventHeader Binary log event header containing basic event details such as event type, event length etc.
	 *            Sample :
	 *            [header=BinlogEventV4HeaderImpl[timestamp=1394108600000,eventType=25,serverId=1,eventLength=85
	 *            ,nextPosition=1501,flags=0,timestampOfReceipt=1394108600580]
	 * @param rowList contains list of all mutated rows
	 * @param dbusOpCode code indicating type of operation such as insertion, update or delete
	 * @param binLogEventMapper mapper corresponding to the source to which event belongs to
	 * @param schema schema corresponding to the source to which event belongs to
	 * @param scn system change number
	 * @return List<DbChangeEntry> list of change records
	 */
	public List<DbChangeEntry> frameAvroRecord(final BinlogEventV4Header eventHeader, final List<Row> rowList,
	        final DbusOpcode dbusOpCode, Map<Integer, BinLogEventMapper<T>> binLogEventMappers, final Schema schema,
	        final long scn)
	{
		List<DbChangeEntry> entryList = new ArrayList<DbChangeEntry>();
		LOGGER.debug("Received frame avro record request for " + eventHeader);
		try
		{
			final long timestampInNanos = eventHeader.getTimestamp() * 1000000L;
			final boolean isReplicated = false;
			for (Row row : rowList)
			{
				List<Column> columns = row.getColumns();
				// getting the appropriate bin log mapper for the logicalSource
				BinLogEventMapper<T> binLogEventMapper =
				        binLogEventMappers.get(lSourceId) == null ? new DefaultBinLogEventMapper<T>(new ORToAvroMapper())
				                : binLogEventMappers.get(lSourceId);

				GenericRecord genericRecord = binLogEventMapper.mapBinLogEvent(eventHeader, row, dbusOpCode, schema);
				List<KeyPair> keyPairList = generateKeyPair(columns, schema);
				DbChangeEntry dbChangeEntry =
				        new DbChangeEntry(scn, timestampInNanos, genericRecord, dbusOpCode, isReplicated, schema,
				                keyPairList);
				entryList.add(dbChangeEntry);
				LOGGER.debug("Successfully Processed the Row " + dbChangeEntry);
			}
		}
		catch (NoSuchSchemaException ne)
		{
			LOGGER.error("No Such element exception : " + ne.getMessage() + " Cause: " + ne.getCause());
			throw new DatabusRuntimeException(ne);
		}
		catch (DatabusException de)
		{
			LOGGER.error("Databus exception : " + de.getMessage() + " Cause: " + de.getCause());
			throw new DatabusRuntimeException(de);
		}
		return entryList;
	}

	/**
	 * Serializes avro record into byte array
	 * @param record generic record
	 * @return serialized byte array
	 * @throws EventCreationException Thrown when event creation failed for a databus source
	 */
	protected byte[] serializeEvent(GenericRecord record) throws EventCreationException
	{
		byte[] serializedValue;
		ByteArrayOutputStream bos = null;
		try
		{
			bos = new ByteArrayOutputStream();
			Encoder encoder = EncoderFactory.get().directBinaryEncoder(bos, null);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
			writer.write(record, encoder);
			serializedValue = bos.toByteArray();
		}
		catch (IOException ex)
		{
			LOGGER.error("Failed to serialize avro record : " + record + " Exception : " + ex.getMessage()
			        + "  Cause: " + ex.getCause());
			throw new EventCreationException("Failed to serialize the Avro GenericRecord", ex);
		}
		catch (RuntimeException ex)
		{
			LOGGER.error("Failed to serialize avro record : " + record + " Exception : " + ex.getMessage()
			        + "  Cause: " + ex.getCause());
			throw new EventCreationException("Failed to serialize the Avro GenericRecord", ex);
		}
		finally
		{
			if (bos != null)
			{
				try
				{
					bos.close();
				}
				catch (IOException e)
				{
					LOGGER.error("Exception occurred while closing output stream");
				}
			}
		}
		return serializedValue;
	}

	/**
	 * Generate Key pairs for the event. Gets primary key information from registered avro schema
	 * and generates key pairs for the given event details
	 * @param columns contains columns related to a mutated row
	 * @param schema schema corresponding to the source to which event belongs to
	 * @return List<KeyPair> list of key pairs corresponding to the event
	 * @throws DatabusException generic Databus exception
	 */
	private List<KeyPair> generateKeyPair(List<Column> columns, Schema schema) throws DatabusException
	{
		Object value = null;
		Schema.Type schemaType = null;
		// get primary key fields from schema
		String pkFieldName = SchemaHelper.getMetaField(schema, "pk");
		LOGGER.debug("Generate Key Pair is called for columns " + columns);
		if (pkFieldName == null)
		{
			LOGGER.error("Primary key not defined for schema " + schema);
			throw new DatabusException("No primary key specified in the schema");
		}
		PrimaryKeySchema pkSchema = new PrimaryKeySchema(pkFieldName);
		List<Schema.Field> fields = schema.getFields();
		List<KeyPair> keyPairList = new ArrayList<KeyPair>();
		int index = 0;
		for (Schema.Field field : fields)
		{
			if (pkSchema.isPartOfPrimaryKey(field))
			{
				value = columns.get(index).getValue();
				schemaType = field.schema().getType();
				KeyPair keyPair = new KeyPair(value, schemaType);
				keyPairList.add(keyPair);
			}
			index++;
		}
		LOGGER.debug("Generated keypairs " + keyPairList + "for columns " + columns);
		return keyPairList;
	}

	/**
	 * Obtains primary key for db change entry
	 * @param dbChangeEntry db change entry
	 * @return key object
	 * @throws DatabusException generic databus exception
	 */
	private Object obtainKey(DbChangeEntry dbChangeEntry) throws DatabusException
	{
		if (null == dbChangeEntry)
		{
			LOGGER.error("Received null dbChangeEntry");
			throw new DatabusException("DBUpdateImage is null");
		}
		List<KeyPair> keyPairList = dbChangeEntry.getPkeys();
		if (null == keyPairList || keyPairList.size() == 0)
		{
			LOGGER.error("Received null dbChangeEntry key pairs");
			throw new DatabusException("There do not seem to be any keys");
		}

		LOGGER.debug("Obtain Key is called for pairs " + keyPairList);
		if (keyPairList.size() == 1)
		{
			Object key = keyPairList.get(0).getKey();
			Schema.Type pKeyType = keyPairList.get(0).getKeyType();
			Object keyObj = null;
			if (pKeyType == Schema.Type.INT)
			{
				if (key instanceof Integer)
				{
					keyObj = key;
				}
				else
				{
					String message = "Schema.Type does not match actual key type (INT) " + key.getClass().getName();
					LOGGER.error(message);
					throw new DatabusException(message);
				}
			}
			else if (pKeyType == Schema.Type.LONG)
			{
				if (key instanceof Long)
				{
					keyObj = key;
				}
				else
				{
					String message = "Schema.Type does not match actual key type (LONG) " + key.getClass().getName();
					LOGGER.error(message);
					throw new DatabusException(message);
				}
				keyObj = key;
			}
			else
			{
				keyObj = key;
			}
			return keyObj;
		}
		else
		{
			/** Treat multiple keys as a separate case to avoid unnecessary casts */
			Iterator<KeyPair> li = keyPairList.iterator();
			StringBuilder compositeKey = new StringBuilder();
			while (li.hasNext())
			{
				KeyPair kp = li.next();
				Schema.Type pKeyType = kp.getKeyType();
				Object key = kp.getKey();
				if (pKeyType == Schema.Type.INT)
				{
					if (key instanceof Integer)
					{
						compositeKey.append(kp.getKey().toString());
					}
					else
					{
						String message = "Schema.Type does not match actual key type (INT) " + key.getClass().getName();
						LOGGER.error(message);
						throw new DatabusException(message);
					}
				}
				else if (pKeyType == Schema.Type.LONG)
				{
					if (key instanceof Long)
					{
						compositeKey.append(key.toString());
					}
					else
					{
						String message =
						        "Schema.Type does not match actual key type (LONG) " + key.getClass().getName();
						LOGGER.error(message);
						throw new DatabusException(message);
					}
				}
				else
				{
					compositeKey.append(key);
				}
				if (li.hasNext())
				{
					/** Add the delimiter for all keys except the last key */
					compositeKey.append(DbusConstants.COMPOUND_KEY_DELIMITER);
				}
			}
			return compositeKey.toString();
		}
	}
}
