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

package com.flipkart.aesop.utils;

import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AvroSchemaHelper
{
	private static final String META_ROW_CHANGE_FIELD = "rowChangeField";
	private static final String META_FIELD_TYPE_NAME = "dbFieldType";
	private static final String PRIMARY_KEY_FIELD_NAME = "pk";

	/**
	 * Returns the fieldname marked as row change field from schema meta
	 * @param schema
	 * @return rowChangeFieldName
	 */
	public static String getRowChangeField(Schema schema)
	{
		return SchemaHelper.getMetaField(schema, META_ROW_CHANGE_FIELD);
	}

	/**
	 * Generates a hash storing fieldName to intended MysqlDataType from schema
	 * @param schema
	 * @return Map containg FieldName to Mysql Type mapping
	 */
	public static Map<String, String> fieldToDataTypeMap(Schema schema)
	{
		Map<String, String> map = new HashMap <String, String>();
		for (Schema.Field field : schema.getFields())
		{
			String mysqlType = SchemaHelper.getMetaField(field, META_FIELD_TYPE_NAME);
			map.put(field.name(), mysqlType);
		}
		return map;
	}

	/**
	 * Generates primary key set using the schema.
	 * @param schema
	 * @return Primary key set
	 * @throws DatabusException
	 */
	public static Set<String> getPrimaryKeysSetFromSchema(Schema schema) throws DatabusException
	{
		String primaryKeyFieldName = SchemaHelper.getMetaField(schema, PRIMARY_KEY_FIELD_NAME);
		if (primaryKeyFieldName == null)
		{
			throw new DatabusException("No primary key specified in the schema");
		}

		Set<String> primaryKeySet = new HashSet<String>();
		for (String primaryKey : primaryKeyFieldName.split(DbusConstants.COMPOUND_KEY_SEPARATOR))
		{
			primaryKeySet.add(primaryKey.trim());
		}
		assert (primaryKeySet.size() >= 1);
		return primaryKeySet;
	}
}

