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

package com.flipkart.aesop.bootstrap.mysql.mapper.impl;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.mapper.BinLogEventMapper;
import com.flipkart.aesop.bootstrap.mysql.utils.ORToMysqlMapper;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.implementation.SourceEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * <code>DefaultBinLogEventMapper</code> maps binlog events to {@link AbstractEvent}
 * @author nrbafna
 */
public class DefaultBinLogEventMapper<T extends AbstractEvent> implements BinLogEventMapper<T>
{
	public static final Logger LOGGER = LogFactory.getLogger(DefaultBinLogEventMapper.class);
	private static String PK_FIELD_NAME = "pk";

	public ORToMysqlMapper orToMysqlMapper;

	public DefaultBinLogEventMapper(ORToMysqlMapper orToMysqlMapper)
	{
		this.orToMysqlMapper = orToMysqlMapper;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T mapBinLogEvent(Row row, Schema schema, DbusOpcode eventType)
	{
		Map<String, Object> keyValuePairs = new HashMap<String, Object>();
		List<Column> columns = row.getColumns();
		List<Schema.Field> orderedFields;

		try
		{
			orderedFields =
			        SchemaHelper.getOrderedFieldsByMetaField(schema, "dbFieldPosition", new Comparator<String>()
			        {

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
				Column column = null;
				if (cnt < columns.size())
				{
					column = columns.get(cnt);
				}
				keyValuePairs.put(field.name(), column == null ? null : orToMysqlMapper.orToMysqlType(column));
				cnt++;
			}

			return (T)new SourceEvent(keyValuePairs, getPkListFromSchema(schema), schema.getName(), schema.getNamespace(),
			        eventType);
		}
		catch (Exception e)
		{
			LOGGER.error(
			        "Error while mapping to MysqlBinLogEvent . Exception : " + e.getMessage() + " Cause: "
			                + e.getCause(), e);
		}
		return null;
	}

	@Override
	public String getUniqueName()
	{
		return this.getClass().getCanonicalName();
	}

	private Set<String> getPkListFromSchema(Schema schema) throws DatabusException
	{
		Set<String> pKeyList = new TreeSet<String>();

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
}
