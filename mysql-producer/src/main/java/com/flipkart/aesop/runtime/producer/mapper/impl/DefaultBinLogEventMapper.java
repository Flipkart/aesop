package com.flipkart.aesop.runtime.producer.mapper.impl;

import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.mapper.BinLogEventMapper;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * <code>DefaultBinLogEventMapper</code> maps binlog events to avro generic record.
 * @author yogesh.dahiya
 * @version 1.0, 26 Mar 2014
 */

public class DefaultBinLogEventMapper implements BinLogEventMapper<GenericRecord>
{
	/** Logger for this class */
	protected static final Logger LOGGER = LogFactory.getLogger(DefaultBinLogEventMapper.class);
	private static String orderingMetaField = "dbFieldPosition";

	/** Open replicator to Avro mapper */
	public ORToAvroMapper orToAvroMapper;

	public DefaultBinLogEventMapper()
	{
	}

	public DefaultBinLogEventMapper(ORToAvroMapper orToAvroMapper)
	{
		this.orToAvroMapper = orToAvroMapper;
	}

	/**
	 * Interface method implementation. Returns the name of this type
	 * @see com.flipkart.aesop.runtime.producer.hbase.SepEventMapper#getUniqueName()
	 */

	public String getUniqueName()
	{
		return this.getClass().getCanonicalName();
	}

	/**
	 * Interface method implementation. Creates {@link GenericRecord} instance based on data in {@link Row} ,
	 * {@link BinlogEventV4Header} and {@link DbusOpcode}
	 * @see org.aesop.runtime.producer.mapper.BinLogEventMapper#mapBinLogEvent(com.google.code.or.binlog.BinlogEventV4Header,com.google.code.or.common.glossary.Row,com.linkedin.databus.core.DbusOpcode)
	 */
	@Override
	public GenericRecord mapBinLogEvent(BinlogEventV4Header header, Row row, DbusOpcode databusCode, Schema schema)
	{
		GenericRecord record = new GenericData.Record(schema);
		List<Column> columns = row.getColumns();
		List<Schema.Field> orderedFields;

		try
		{
			orderedFields =
			        SchemaHelper.getOrderedFieldsByMetaField(schema, orderingMetaField, new Comparator<String>()
			        {

				        @Override
				        public int compare(String o1, String o2)
				        {
					        Integer pos1 = Integer.parseInt(o1);
					        Integer pos2 = Integer.parseInt(o2);

					        return pos1.compareTo(pos2);
				        }
			        });
			int cnt = 0;
			for (Schema.Field field : orderedFields)
			{
				Column column = columns.get(cnt);
				record.put(field.name(), column == null ? null : orToAvroMapper.orToAvroType(column));
				cnt++;
			}
			LOGGER.info("Mapped GenricRecord : " + record.toString());
			return record;
		}
		catch (Exception e)
		{
			LOGGER.error("Error while mapping to DefaultBinlogEvent . Exception : " + e.getMessage() + " Cause: "
			        + e.getCause());
		}
		return null;
	}

	/** Getters/Setters */
	public ORToAvroMapper getOrToAvroMapper()
	{
		return orToAvroMapper;
	}

	public void setOrToAvroMapper(ORToAvroMapper orToAvroMapper)
	{
		this.orToAvroMapper = orToAvroMapper;
	}

}
