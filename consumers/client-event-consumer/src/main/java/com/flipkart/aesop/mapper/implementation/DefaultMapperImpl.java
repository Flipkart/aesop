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

import java.util.List;
import java.util.Set;

import org.springframework.util.Assert;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.mapper.AbstractMapper;
import com.flipkart.aesop.mapper.Mapper;
import com.flipkart.aesop.mapper.config.implementation.MapperConfigImpl;
import com.flipkart.aesop.mapper.enums.EntityExistInConfig;
import com.flipkart.aesop.mapper.enums.NamespaceExistInConfig;
import com.flipkart.aesop.mapper.utils.MapperHelper;
import com.typesafe.config.Config;

/**
 * Default Implementation of the {@link Mapper}. Uses {@link MapperConfigImpl}.
 * @author Prakhar Jain
 */
public class DefaultMapperImpl extends AbstractMapper
{
	/** {@link Config} generated from the HOCON-config. */
	private Config config;

	/** Sets the required Singleton instances for the {@link MapperType#values()}. */
	private void setBeansForMapperType()
	{
		for (MapperType mapperType : mapperTypeList)
		{
			mapperType.setMapperConfig(mapperConfig);
			mapperType.setEventGroupFinder(eventGroupFinder);
			mapperType.setDestinationEventFactory(destinationEventFactory);
		}
	}

	/**
	 * Chooses appropriate {@link MapperType} based on whether namespace and entity exists in the HOCON-config or not.
	 * {@link MapperType} covers all possible cases. <em> Hence, this function should never return {@code null}. </em>
	 * @param namespacePath
	 * @param entityPath
	 * @return {@link MapperType#values()}
	 */
	private MapperType chooseMapperBasedOnConfig(String namespacePath, String entityPath)
	{
		Boolean namespacePathExist = mapperConfig.checkIfPathExists(namespacePath, config);
		Boolean entityPathExist = mapperConfig.checkIfPathExists(entityPath, config);

		NamespaceExistInConfig namespaceExistInConfig =
		        NamespaceExistInConfig.valueOf(String.valueOf(namespacePathExist).toUpperCase());
		EntityExistInConfig entityExistInConfig =
		        EntityExistInConfig.valueOf(String.valueOf(entityPathExist).toUpperCase());

		setBeansForMapperType();

		for (MapperType mapperType : mapperTypeList)
		{
			if (mapperType.namespacePathExists == namespaceExistInConfig
			        && mapperType.entityPathExists == entityExistInConfig)
			{
				return mapperType;
			}
		}

		return null;
	}

	@Override
	public List<AbstractEvent> mapSourceEventToDestinationEvent(AbstractEvent sourceEvent,
	        Set<Integer> destinationGroupSet, int totalDestinationGroups)
	{
		config = mapperConfig.getConfig(configFilePath);

		String namespacePath = MapperHelper.getNamespacePath(configRoot, sourceEvent);
		String entityPath = MapperHelper.getEntityPath(configRoot, sourceEvent);

		MapperType mapperType = chooseMapperBasedOnConfig(namespacePath, entityPath);

		Assert.notNull(mapperType);

		return mapperType.map(config, configRoot, sourceEvent, destinationGroupSet, totalDestinationGroups);
	}
}
