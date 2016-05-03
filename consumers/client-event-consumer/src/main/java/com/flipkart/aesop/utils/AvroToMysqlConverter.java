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

import java.util.HashMap;
import java.util.Map;

public abstract class AvroToMysqlConverter
{
    /**
   	 * This returns mysql object from avro object and intended mysql datatype
   	 * @param mysqlType
   	 * @param fieldValue
   	 * @return MysqlTypedObject using AvroToMysqlMapper
   	 */
   	public static Object getMysqlTypedObject(String mysqlType, Object fieldValue)
   	{
   		return AvroToMysqlMapper.avroToMysqlType(fieldValue, MysqlDataTypes.valueOf(mysqlType.toUpperCase()));
   	}

   	/**
   	 * This specifically handles rowChangeField which comes in form HashMap and converting each key/value to Mysql type
   	 * @param fieldMap
   	 * @param fieldToMysqlDataType
   	 * @return MysqlTypedObject using AvroToMysqlMapper
   	 */
   	public static Map<String, Object> getMysqlTypedObjectForMap(Map<Object, Object> fieldMap,
                                                         Map<String, String> fieldToMysqlDataType)
   	{
   		Map<String, Object> mysqlTypedObject = null;
   		if (fieldMap != null)
   		{
   			mysqlTypedObject = new HashMap<String, Object>(fieldMap.size());
   			for (Object key : fieldMap.keySet())
   			{
   				String fieldName = key.toString();
   				String sqlType = fieldToMysqlDataType.get(fieldName);
   				mysqlTypedObject.put(fieldName, AvroToMysqlConverter.getMysqlTypedObject(sqlType, fieldMap.get(key)));
   			}
   		}
   		return mysqlTypedObject;
   	}
}
