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

package com.flipkart.aesop.mapper.implementation;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.implementation.DestinationEventFactory;
import com.flipkart.aesop.event.implementation.SourceEvent;
import com.flipkart.aesop.mapper.config.MapperConfig;
import com.flipkart.aesop.mapper.enums.EntityExistInConfig;
import com.flipkart.aesop.mapper.enums.MapAllValues;
import com.flipkart.aesop.mapper.enums.NamespaceExistInConfig;
import com.flipkart.aesop.mapper.eventGroupFilter.EventGroupFinder;
import com.flipkart.aesop.mapper.utils.MapperConstants;
import com.flipkart.aesop.mapper.utils.MapperHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

import java.util.*;

/**
 * Implements the logic of mapping from {@link SourceEvent} to {@link List <DestinationEvent>} for various
 * cases.
 * @author Prakhar Jain
 * @see DefaultMapperImpl
 */
public enum MapperType
{
	/**
	 * Implements the case in which the <a
	 * href="https://github.com/typesafehub/config#using-hocon-the-json-superset">HOCON-config</a> of the
	 * {@code namespace} and {@code entity} of the {@code SourceEvent} is not defined, that is, {root.namespace} path,
	 * and hence, {root.namespace.entity} path in
	 * HOCON-config does
	 * not exist.
	 * <p>
	 * In this case, a {@link List <AbstractEvent>} with either 0 or 1 Destination Event is returned. 0 in case when
	 * namespace of the {@code SourceEvent} is present in the {@link MapperConstants#EXCLUSION_LIST_FIELD_NAME} defined inside
	 * the root in HOCON-config. 1 in the other case.
	 */
	MAP_WITHOUT_SOURCE_NAMESPACE_ENTITY_CONFIG(NamespaceExistInConfig.FALSE, EntityExistInConfig.FALSE)
	{
		@Override
		public List<AbstractEvent> map(Config config, String configRoot, AbstractEvent sourceEvent,
		        Set<Integer> destinationGroupSet, int totalDestinationGroups)
		{
			ConfigValue configValue = mapperConfig.getConfigValue(config, configRoot);

			Set<String> exclusionSet = getExclusionSet(configValue);
			MapAllValues mapAll = getMapAll(configValue);

			if (mapAll == MapAllValues.FALSE || exclusionSet.contains(sourceEvent.getNamespaceName()))
			{
				return new ArrayList<AbstractEvent>();
			}

			AbstractEvent destinationEvent =
			        destinationEventFactory.createEvent(sourceEvent.getFieldMapPair(), sourceEvent.getPrimaryKeySet(),
			                sourceEvent.getEntityName(), sourceEvent.getNamespaceName(), sourceEvent.getEventType());

			return Arrays.asList(destinationEvent);
		}
	},
	/**
	 * Implements the case in which the <a
	 * href="https://github.com/typesafehub/config#using-hocon-the-json-superset">HOCON-config</a> of the {@code entity}
	 * of the {@code SourceEvent} is not defined, that is, {root.namespace} path exists,
	 * but, {root.namespace.entity} path in
	 * HOCON-config does
	 * not exist.
	 * <p>
	 * In this case, a {@link List <AbstractEvent>} with either 0 or 1 Destination Event is returned. 0 in case when
	 * entity of the {@code SourceEvent} is present in the {@link MapperConstants#EXCLUSION_LIST_FIELD_NAME} defined inside the
	 * namespace in HOCON-config. 1 in the other case.
	 */
	MAP_WITHOUT_SOURCE_ENTITY_CONFIG(NamespaceExistInConfig.TRUE, EntityExistInConfig.FALSE)
	{
		@Override
		public List<AbstractEvent> map(Config config, String configRoot, AbstractEvent sourceEvent,
		        Set<Integer> destinationGroupSet, int totalDestinationGroups)
		{
			String namespacePath = MapperHelper.getNamespacePath(configRoot, sourceEvent);
			ConfigValue configValue = mapperConfig.getConfigValue(config, namespacePath);

			Set<String> exclusionSet = getExclusionSet(configValue);
			MapAllValues mapAll = getMapAll(configValue);

			if (mapAll == MapAllValues.FALSE || exclusionSet.contains(sourceEvent.getEntityName()))
			{
				return new ArrayList<AbstractEvent>();
			}

			AbstractEvent destinationEvent =
			        destinationEventFactory.createEvent(sourceEvent.getFieldMapPair(), sourceEvent.getPrimaryKeySet(),
			                sourceEvent.getEntityName(), sourceEvent.getNamespaceName(), sourceEvent.getEventType());

			return Arrays.asList(destinationEvent);
		}
	},
	/**
	 * Implements the case in which the <a
	 * href="https://github.com/typesafehub/config#using-hocon-the-json-superset">HOCON-config</a> of the {@code entity}
	 * and hence the {@code namespace} of the {@code SourceEvent} is defined, that is, {root.namespace} path,
	 * and, {root.namespace.entity} path in
	 * HOCON-config exist.
	 * <p>
	 * In this case, the number of events in {@link List <AbstractEvent>} will be equal to the number of elements in the
	 * list defined in the HOCON-config for {root.namespace.entity} path. Destination Namespace, Destination Entity,
	 * Group Id, Primary Keys, Column Mapping can be explicitly defined in this case.
	 */
	MAP_WITH_SOURCE_ENTITY_CONFIG(NamespaceExistInConfig.TRUE, EntityExistInConfig.TRUE)
	{
		@Override
		public List<AbstractEvent> map(Config config, String configRoot, AbstractEvent sourceEvent,
		        Set<Integer> destinationGroupSet, int totalDestinationGroups)
		{
			List<AbstractEvent> destinationEventList = new ArrayList<AbstractEvent>();

			String entityPath = MapperHelper.getEntityPath(configRoot, sourceEvent);

			ConfigList configList = mapperConfig.getConfigList(config, entityPath);

			for (ConfigValue destinationConfig : configList)
			{
				String destinationNamespace = sourceEvent.getNamespaceName();
				if (mapperConfig.checkIfKeyExists(MapperConstants.DESTINATION_NAMESPACE_FIELD_NAME, destinationConfig))
				{
					destinationNamespace =
					        mapperConfig.getValueForKeyAsString(MapperConstants.DESTINATION_NAMESPACE_FIELD_NAME,
					                destinationConfig);
				}

				String destinationEntity = sourceEvent.getEntityName();

				if (mapperConfig.checkIfKeyExists(MapperConstants.DESTINATION_ENTITY_FIELD_NAME, destinationConfig))
				{
					destinationEntity =
					        mapperConfig
					                .getValueForKeyAsString(MapperConstants.DESTINATION_ENTITY_FIELD_NAME, destinationConfig);
				}

				int groupNo =
				        getEventGroupNo(totalDestinationGroups, destinationConfig, destinationNamespace,
				                destinationEntity);

				if (!destinationGroupSet.contains(groupNo))
				{
					continue;
				}

				MapAllValues mapAll = getMapAll(destinationConfig);
				Set<String> exclusionSet = getExclusionSet(destinationConfig);
				Map<String, Object> destinationEventColumnMap =
				        getColumnMap(sourceEvent, destinationConfig, mapAll, exclusionSet);
				Set<String> primaryKeySet = getPrimaryKeySet(sourceEvent, destinationConfig);

				AbstractEvent destinationEvent =
				        destinationEventFactory.createEvent(destinationEventColumnMap, primaryKeySet,
				                destinationEntity, destinationNamespace, sourceEvent.getEventType());

				destinationEventList.add(destinationEvent);
			}
			return destinationEventList;
		}
	};

	/**
	 * Gets the array of primary keys from the {@code ConfigValue} if exists, otherwise, returns the primary keys of the
	 * passed source event.
	 * @param sourceEvent
	 * @param destinationConfig
	 * @return Set of primary key columns
	 */
	protected Set<String> getPrimaryKeySet(AbstractEvent sourceEvent, ConfigValue destinationConfig)
	{
		Set<String> primaryKeySet;
		if (mapperConfig.checkIfKeyExists(MapperConstants.DESTINATION_PRIMARY_KEY_LIST, destinationConfig))
		{
			ConfigList primaryKeyConfigObject =
			        mapperConfig.getValueForKeyAsConfigValue(MapperConstants.DESTINATION_PRIMARY_KEY_LIST, destinationConfig,
			                ConfigList.class);

			primaryKeySet = new HashSet<String>();

			for (ConfigValue configValue : primaryKeyConfigObject)
			{
				if (configValue.unwrapped() != null)
				{
					primaryKeySet.add(configValue.unwrapped().toString());
				}
			}
		}
		else
		{
			primaryKeySet = sourceEvent.getPrimaryKeySet();
		}
		return primaryKeySet;
	}

	/**
	 * Gets the map of column mapping from the {@link ConfigValue} if exists, otherwise, returns the primary keys of the
	 * passed source event, which varies as per the values of mapAll and exclusionList defined in the
	 * {@code ConfigValue}.
	 * @param sourceEvent
	 * @param destinationConfig
	 * @param mapAll
	 * @param exclusionSet
	 * @return Source to Destination Event Column Mapping
	 */
	protected Map<String, Object> getColumnMap(AbstractEvent sourceEvent, ConfigValue destinationConfig,
	        MapAllValues mapAll, Set<String> exclusionSet)
	{
		Map<String, Object> destinationEventColumnMap = null;
		if (mapperConfig.checkIfKeyExists(MapperConstants.DESTINATION_COLUMN_MAPPING, destinationConfig))
		{
			ConfigObject columnMappingConfigObject =
			        mapperConfig.getValueForKeyAsConfigValue(MapperConstants.DESTINATION_COLUMN_MAPPING, destinationConfig,
			                ConfigObject.class);

			destinationEventColumnMap =
			        MapperHelper.getEventColumnMapping(sourceEvent.getFieldMapPair(),
							columnMappingConfigObject, mapAll, exclusionSet);
		}
		else
		{
			destinationEventColumnMap =
			        MapperHelper.getEventColumnMapping(sourceEvent.getFieldMapPair(), null, mapAll,
							exclusionSet);
		}
		return destinationEventColumnMap;
	}

	/**
	 * Gets the exclusion set for the destination {@link ConfigValue}.
	 * @param destinationConfig
	 * @return Exclusion Set
	 */
	protected Set<String> getExclusionSet(ConfigValue destinationConfig)
	{
		Set<String> exclusionSet = new HashSet<String>();
		if (mapperConfig.checkIfKeyExists(MapperConstants.EXCLUSION_LIST_FIELD_NAME, destinationConfig))
		{
			ConfigList exclusionListConfigObject =
			        mapperConfig.getValueForKeyAsConfigValue(MapperConstants.EXCLUSION_LIST_FIELD_NAME, destinationConfig,
			                ConfigList.class);

			exclusionSet = new HashSet<String>();

			for (ConfigValue configValue : exclusionListConfigObject)
			{
				if (configValue.unwrapped() != null)
				{
					exclusionSet.add(configValue.unwrapped().toString());
				}
			}
		}
		return exclusionSet;
	}

	/**
	 * Gets the mapAll value for the destination {@link ConfigValue}.
	 * @param destinationConfig
	 * @return MapAll Value
	 */
	protected MapAllValues getMapAll(ConfigValue destinationConfig)
	{
		MapAllValues mapAll = MapAllValues.FALSE;
		if (mapperConfig.checkIfKeyExists(MapperConstants.MAP_ALL_FIELD_NAME, destinationConfig))
		{
			String mapAllValue = mapperConfig.getValueForKeyAsString(MapperConstants.MAP_ALL_FIELD_NAME, destinationConfig);

			mapAll = MapAllValues.valueOf(mapAllValue.toUpperCase());
		}
		return mapAll;
	}

	/**
	 * Gets the event group no from the config if exists, otherwise, using the {@link EventGroupFinder}.
	 * @param totalDestinationGroups
	 * @param destinationConfig
	 * @param destinationNamespace
	 * @param destinationEntity
	 * @return Event Group No
	 */
	protected int getEventGroupNo(int totalDestinationGroups, ConfigValue destinationConfig,
	        String destinationNamespace, String destinationEntity)
	{
		int groupNo = 1;

		if (mapperConfig.checkIfKeyExists(MapperConstants.DESTINATION_ENTITY_GROUP_NO_FIELD_NAME, destinationConfig))
		{
			String configGroupNo =
			        mapperConfig.getValueForKeyAsString(MapperConstants.DESTINATION_ENTITY_GROUP_NO_FIELD_NAME,
			                destinationConfig);

			groupNo = Integer.parseInt(configGroupNo);
		}
		else
		{
			groupNo = eventGroupFinder.getEventGroupNo(destinationNamespace, destinationEntity, totalDestinationGroups);
		}
		return groupNo;
	}

	protected NamespaceExistInConfig namespacePathExists;
	protected EntityExistInConfig entityPathExists;
	protected MapperConfig mapperConfig;
	protected EventGroupFinder eventGroupFinder;
	protected DestinationEventFactory destinationEventFactory;

	private MapperType(NamespaceExistInConfig namespacePathExists, EntityExistInConfig entityPathExists)
	{
		this.namespacePathExists = namespacePathExists;
		this.entityPathExists = entityPathExists;
	}

	public abstract List<AbstractEvent> map(Config config, String configRoot, AbstractEvent sourceEvent,
	        Set<Integer> destinationGroupSet, int totalDestinationGroups);

	/**
	 * Sets the Mapper Config.
	 * @param mapperConfig
	 */
	public void setMapperConfig(MapperConfig mapperConfig)
	{
		this.mapperConfig = mapperConfig;
	}

	/**
	 * Sets the Event Group Finder.
	 * @param eventGroupFinder
	 */
	public void setEventGroupFinder(EventGroupFinder eventGroupFinder)
	{
		this.eventGroupFinder = eventGroupFinder;
	}

	/**
	 * Sets the Destination Event Factory class object.
	 * @param destinationEventFactory
	 */
	public void setDestinationEventFactory(DestinationEventFactory destinationEventFactory)
	{
		this.destinationEventFactory = destinationEventFactory;
	}
}
