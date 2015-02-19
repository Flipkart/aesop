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

package com.flipkart.aesop.mapper.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.mapper.enums.MapAllValues;
import com.flipkart.aesop.mapper.implementation.DefaultMapperImpl;
import com.flipkart.aesop.mapper.implementation.MapperType;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

/**
 * Provides static functions to be used in Mapper Logic in {@link DefaultMapperImpl}, {@link MapperType}.
 * @author Prakhar Jain
 */
public class MapperHelper
{
	/**
	 * Gets Namespace mapAll Path.
	 * @param configRoot
	 * @return Namespace mapAll Path
	 */
	public static String getNamespaceMapAllPath(String configRoot)
	{
		String namespaceMapAllPath =
		        configRoot + MapperConstants.CONFIG_PATH_SEPARATOR + MapperConstants.MAP_ALL_FIELD_NAME;
		return namespaceMapAllPath;
	}

	/**
	 * Gets Column mapAll Path.
	 * @param configRoot
	 * @param sourceEvent
	 * @return Column mapAll Path
	 */
	public static String getColumnMapAllPath(String configRoot, AbstractEvent sourceEvent)
	{
		String columnMapAllPath =
		        getEntityPath(configRoot, sourceEvent) + MapperConstants.CONFIG_PATH_SEPARATOR
		                + MapperConstants.MAP_ALL_FIELD_NAME;
		return columnMapAllPath;
	}

	/**
	 * Gets Column exclusionList Path.
	 * @param configRoot
	 * @param sourceEvent
	 * @return Column exclusionList Path
	 */
	public static String getColumnExclusionListPath(String configRoot, AbstractEvent sourceEvent)
	{
		String columnExclusionListPath =
		        getEntityPath(configRoot, sourceEvent) + MapperConstants.CONFIG_PATH_SEPARATOR
		                + MapperConstants.EXCLUSION_LIST_FIELD_NAME;
		return columnExclusionListPath;
	}

	/**
	 * Gets Entity mapAll Path.
	 * @param configRoot
	 * @param event
	 * @return Entity mapAll Path
	 */
	public static String getEntityMapAllPath(String configRoot, AbstractEvent event)
	{
		String entityMapAllPath =
		        getNamespacePath(configRoot, event) + MapperConstants.CONFIG_PATH_SEPARATOR
		                + MapperConstants.MAP_ALL_FIELD_NAME;
		return entityMapAllPath;
	}

	/**
	 * Gets Entity exclusionList Path.
	 * @param configRoot
	 * @param event
	 * @return Entity exclusionList Path
	 */
	public static String getEntityExclusionListPath(String configRoot, AbstractEvent event)
	{
		String entityExclusionListPath =
		        getNamespacePath(configRoot, event) + MapperConstants.CONFIG_PATH_SEPARATOR
		                + MapperConstants.EXCLUSION_LIST_FIELD_NAME;
		return entityExclusionListPath;
	}

	/**
	 * Gets Namespace exclusionList Path.
	 * @param configRoot
	 * @return Namespace exclusionList Path
	 */
	public static String getNamespaceExclusionListPath(String configRoot)
	{
		String namespaceExclusionListPath =
		        configRoot + MapperConstants.CONFIG_PATH_SEPARATOR + MapperConstants.EXCLUSION_LIST_FIELD_NAME;
		return namespaceExclusionListPath;
	}

	/**
	 * Gets Namespace Path.
	 * @param configRoot
	 * @param event
	 * @return Namespace Path
	 */
	public static String getNamespacePath(String configRoot, AbstractEvent event)
	{
		String namespacePath = configRoot + MapperConstants.CONFIG_PATH_SEPARATOR + event.getNamespaceName();
		return namespacePath;
	}

	/**
	 * Gets Entity Path.
	 * @param configRoot
	 * @param event
	 * @return Entity Path.
	 */
	public static String getEntityPath(String configRoot, AbstractEvent event)
	{
		String entityPath =
		        getNamespacePath(configRoot, event) + MapperConstants.CONFIG_PATH_SEPARATOR + event.getEntityName();
		return entityPath;
	}

	/**
	 * Gets Entity groupNo Path.
	 * @param configRoot
	 * @param event
	 * @return Entity groupNo Path
	 */
	public static String getEntityGroupNoPath(String configRoot, AbstractEvent event)
	{
		String entityPath =
		        getEntityPath(configRoot, event) + MapperConstants.CONFIG_PATH_SEPARATOR
		                + MapperConstants.DESTINATION_ENTITY_GROUP_NO_FIELD_NAME;
		return entityPath;
	}

	/**
	 * Gets columnMap Path.
	 * @param configRoot
	 * @param event
	 * @return columnMap Path
	 */
	public static String getColumnMapPath(String configRoot, AbstractEvent event)
	{
		String entityPath =
		        getEntityPath(configRoot, event) + MapperConstants.CONFIG_PATH_SEPARATOR
		                + MapperConstants.DESTINATION_COLUMN_MAPPING;
		return entityPath;
	}

	/**
	 * Gets primaryKeyList Path.
	 * @param configRoot
	 * @param event
	 * @return primaryKeyList Path
	 */
	public static String getPrimaryKeyListPath(String configRoot, AbstractEvent event)
	{
		String entityPath =
		        getEntityPath(configRoot, event) + MapperConstants.CONFIG_PATH_SEPARATOR
		                + MapperConstants.DESTINATION_PRIMARY_KEY_LIST;
		return entityPath;
	}

	/**
	 * Generates the Destination Event Column Mapping using source column mapping, and, exclusionList and mapAll
	 * specified in the HOCON-config.
	 * @param sourceEventColumnMap
	 * @param columnMappingConfigObject
	 * @param mapAll
	 * @param exclusionSet
	 * @return Destination Event Column Mapping
	 */
	public static Map<String, Object> getDestinationEventColumnMapping(Map<String, Object> sourceEventColumnMap,
	        ConfigObject columnMappingConfigObject, MapAllValues mapAll, Set<String> exclusionSet)
	{
		Map<String, Object> destinationColumnMap = new HashMap<String, Object>();

		for (Entry<String, Object> sourceColumnEntry : sourceEventColumnMap.entrySet())
		{
			String sourceColumnName = sourceColumnEntry.getKey();
			Object sourceColumnValue = sourceColumnEntry.getValue();

			if (columnMappingConfigObject != null && columnMappingConfigObject.containsKey(sourceColumnName))
			{
				ConfigValue sourceColumnConfig = columnMappingConfigObject.get(sourceColumnName);
				String destinationColumn = sourceColumnConfig.unwrapped().toString();
                if (destinationColumn.contains("."))
                {
                    String destinationNestedField[] = destinationColumn.split("\\.");
                    Map<String,Object> destinationFieldMap = (destinationColumnMap.containsKey(destinationNestedField[0]) ?
                            (Map<String,Object>)destinationColumnMap.get(destinationNestedField[0]) : new HashMap<String,Object>() );
                    destinationFieldMap.put(destinationNestedField[1],sourceColumnValue);
                    destinationColumnMap.put(destinationNestedField[0],destinationFieldMap);
                }
                else
                {
                    destinationColumnMap.put(destinationColumn, sourceColumnValue);
                }
			}
			else if (mapAll == MapAllValues.TRUE && !exclusionSet.contains(sourceColumnName))
			{
				destinationColumnMap.put(sourceColumnName, sourceColumnValue);
			}
		}

		return destinationColumnMap;
	}
}
