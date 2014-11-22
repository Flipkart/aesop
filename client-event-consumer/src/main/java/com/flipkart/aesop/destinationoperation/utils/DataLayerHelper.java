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

package com.flipkart.aesop.destinationoperation.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Data Layer static Helper Functions go here.
 * @author Prakhar Jain
 */
public class DataLayerHelper
{
	/**
	 * Generates a map which has non-primary fields as null.
	 * @param fieldMap
	 * @param primaryKeySet
	 * @return the Map with non-primary fields as null.
	 */
	public static Map<String, Object> generateColumnMappingWithNullValues(Map<String, Object> fieldMap,
	        Set<String> primaryKeySet)
	{
		Map<String, Object> nullValueColumnMapping = new HashMap<String, Object>();

		for (String columnName : fieldMap.keySet())
		{
			if (primaryKeySet.contains(columnName))
			{
				nullValueColumnMapping.put(columnName, fieldMap.get(columnName));
			}
			else
			{
				nullValueColumnMapping.put(columnName, null);
			}
		}

		return nullValueColumnMapping;
	}
}
