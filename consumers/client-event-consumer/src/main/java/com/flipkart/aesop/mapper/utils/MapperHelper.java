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

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.mapper.enums.MapAllValues;
import com.flipkart.aesop.mapper.implementation.DefaultMapperImpl;
import com.flipkart.aesop.mapper.implementation.MapperType;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

import java.util.*;
import java.util.Map.Entry;

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
	@SuppressWarnings("unchecked")
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
                /*Check if Destination Column is of type Column...NestedField*/
                if (destinationColumn.contains("."))
                {
                    String destinationNestedField[] = destinationColumn.split("\\.");
                    /*destinationNestedField[0] has Column, destinationNestedField[1] has NestedField1,
                    destinationNestedField[2] has NestedField2 ... */
                    Map<String,Object> destinationFieldMap = destinationColumnMap; /* has reference to higherup field map*/
                    Map<String,Object> nestedFieldMap;                             /* has reference to nested field map */
                    for(int i=0; i<destinationNestedField.length-1;i++)
                    {
                        /*get current field if already exists, else create new field*/
                        nestedFieldMap = (destinationFieldMap.containsKey(destinationNestedField[i]) ?
                                (Map<String,Object>)destinationFieldMap.get(destinationNestedField[i])  : new HashMap<String,Object>());
                        if(nestedFieldMap.isEmpty()) {
                            /*insert in higherup field if empty*/
                            destinationFieldMap.put(destinationNestedField[i],nestedFieldMap);
                        }

                        destinationFieldMap=nestedFieldMap;  /*point higherup field map to current nestedFieldMap*/
                        if(i==destinationNestedField.length-2) {
                            /*if 1 level up then lowest level, put the lowest level in current fieldmap*/
                            nestedFieldMap.put(destinationNestedField[destinationNestedField.length-1],sourceColumnValue);
                        }
                    }
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

	/**
	 * Generates destination event column mapping using <code>columnMappingConfig</code>, <code>exclusionSet</code> and <code>mapAll</code>.<br>
	 * Allows mapping source fields to any r-th level destination list field.
	 * @param sourceEventColumnMap k-v map of source event attributes
	 * @param columnMappingConfigObject HOCON config for mapping src event to destination event.
	 * @param mapAll flag whether as-is src to dest event field mapping should be used
	 * @param exclusionSet set of src fields which needs to be excluded if <code>mapAll</code> is set to MapAllValues.TRUE
	 * @return destination event field mapping
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getEventColumnMapping(Map<String, Object> sourceEventColumnMap, ConfigObject columnMappingConfigObject,
															MapAllValues mapAll, Set<String> exclusionSet) {

		Map<String, Object> destinationColumnMap = new HashMap<String, Object>();

		for(String sourceColumn: sourceEventColumnMap.keySet()) {
			if(null != columnMappingConfigObject && columnMappingConfigObject.containsKey(sourceColumn)) {

				String sourceColumnConfig = columnMappingConfigObject.get(sourceColumn).unwrapped().toString();
				Object sourceColumnValue = sourceEventColumnMap.get(sourceColumn);

				//Check if destination column type is nested
				if(sourceColumnConfig.contains(".")) {
					String destinationNestedFields[] = sourceColumnConfig.split("\\.");

					List<Object> nestedFieldList = null;
					Map<String, Object> nestedFieldMap = destinationColumnMap;
					boolean currLevelMap = true;

					for(int w = 0; w < destinationNestedFields.length ; ++w) {

						final String wLevelField = destinationNestedFields[w];

						//Check if destination current w-th level field is a list mapping
						if(wLevelField.endsWith("[]")) {

							String fieldName = wLevelField.replace("[]", "");
							//Check if previous level under which current w-th level field is
							//contained is a map
							if(currLevelMap) {

								List<Object> wthLevelFieldList = nestedFieldMap.containsKey(fieldName) ?
										(ArrayList<Object>)nestedFieldMap.get(fieldName) : new ArrayList<Object>();

								if(destinationNestedFields.length-1 == w)
									wthLevelFieldList.add(sourceColumnValue);

								nestedFieldMap.put(fieldName, wthLevelFieldList);
								nestedFieldList = wthLevelFieldList;

							//Alternatively previous level under which current w-th level field is
							//contained is a list
							} else {

								Map<String, Object> wthLevelFieldMap = nestedFieldList.iterator().hasNext() ?
										(HashMap<String, Object>)nestedFieldList.iterator().next() : new HashMap<String, Object>();

								nestedFieldList.clear();
								nestedFieldList.add(wthLevelFieldMap);

								List<Object> innerFieldSet = wthLevelFieldMap.containsKey(fieldName) ? (ArrayList<Object>)wthLevelFieldMap.get(fieldName) : new ArrayList<Object>();

								if(destinationNestedFields.length-1 == w)
									innerFieldSet.add(sourceColumnValue);

								wthLevelFieldMap.put(fieldName, innerFieldSet);
								nestedFieldList = innerFieldSet;
							}

							currLevelMap = false;
						//If otherwise the current destination w-th level field is a non list-mapping
						} else {

							//Check if previous level under which current w-th level field is
							//contained is a map
							if(currLevelMap) {

								if(destinationNestedFields.length-1 == w) {
									nestedFieldMap.put(wLevelField, sourceColumnValue);
									break;
								}

								Map<String, Object> wthLevelFieldMap = nestedFieldMap.containsKey(wLevelField) ?
										(HashMap<String, Object>)nestedFieldMap.get(wLevelField) : new HashMap<String, Object>();
								nestedFieldMap.put(wLevelField, wthLevelFieldMap);
								nestedFieldMap = wthLevelFieldMap;

							//Alternatively previous level under which current w-th level field is
							//contained is a list
							} else {

								HashMap<String, Object> wthLevelFieldMap = (nestedFieldList.iterator().hasNext()) ?
										(HashMap<String, Object>) nestedFieldList.iterator().next() :
										new HashMap<String, Object>();

								nestedFieldList.clear();
								nestedFieldList.add(wthLevelFieldMap);

								if(destinationNestedFields.length-1 == w) {
									wthLevelFieldMap.put(wLevelField, sourceColumnValue);
									break;
								}

								HashMap<String, Object> innerFieldMap = (wthLevelFieldMap.containsKey(wLevelField)) ?
										(HashMap<String, Object>) wthLevelFieldMap.get(wLevelField) : new HashMap<String, Object>();

								wthLevelFieldMap.put(wLevelField, innerFieldMap);
								nestedFieldMap = innerFieldMap;
							}

							currLevelMap = true;
						}
					}

				//Check if destination column type is non-nested
				} else {
					//destination column is a list mapping
					if(sourceColumnConfig.endsWith("[]")) {

						String columnName = sourceColumnConfig.replace("[]", "");
						List<Object> fieldValues = (destinationColumnMap.containsKey(columnName)) ? (ArrayList<Object>) destinationColumnMap.get(columnName) : new ArrayList<Object>();
						fieldValues.add(sourceColumnValue);
						destinationColumnMap.put(columnName, fieldValues);

					//destination column is a map
					} else {

						destinationColumnMap.put(sourceColumnConfig, sourceColumnValue);
					}

				}

			} else if (MapAllValues.TRUE == mapAll && !exclusionSet.contains(sourceColumn)){
				destinationColumnMap.put(sourceColumn, sourceEventColumnMap.get(sourceColumn));
			}
		}

		return destinationColumnMap;
	}
}
