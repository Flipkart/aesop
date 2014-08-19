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
 * @author yogesh.dahiya, jagadeesh.huliyar
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
			LOGGER.info("Mapped GenricRecord for schema " + schema.getName() + " : " + record.toString());
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
