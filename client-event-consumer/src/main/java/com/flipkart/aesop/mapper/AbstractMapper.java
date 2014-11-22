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

package com.flipkart.aesop.mapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.implementation.DestinationEventFactory;
import com.flipkart.aesop.mapper.config.MapperConfig;
import com.flipkart.aesop.mapper.eventGroupFilter.EventGroupFinder;
import com.flipkart.aesop.mapper.implementation.DefaultMapperImpl;
import com.flipkart.aesop.mapper.implementation.MapperType;

/**
 * Adapter class for {@link Mapper}.
 * @author Prakhar Jain
 * @see DefaultMapperImpl
 */
public abstract class AbstractMapper implements Mapper
{
	/** Path for HOCON-config file. */
	protected String configFilePath;
	/** Root of the HOCON-config file. */
	protected String configRoot;
	/** Mapper Config Instance. */
	protected MapperConfig mapperConfig;
	/** List of {@link MapperType}. */
	protected List<MapperType> mapperTypeList;
	/** Event Group Finder instance. */
	protected EventGroupFinder eventGroupFinder;
	/** Destination Event Factory */
	protected DestinationEventFactory destinationEventFactory;

	public abstract List<AbstractEvent> mapSourceEventToDestinationEvent(AbstractEvent sourceEvent,
	        Set<Integer> destinationGroupSet, int totalDestinationGroups);

	public List<AbstractEvent> mapSourceEventToDestinationEvent(AbstractEvent sourceEvent)
	{
		Set<Integer> destinationGroupSet = new HashSet<Integer>();
		destinationGroupSet.add(1);
		return mapSourceEventToDestinationEvent(sourceEvent, destinationGroupSet, 1);
	}

	/**
	 * Sets the Config File Path.
	 * @param configFilePath
	 */
	public void setConfigFilePath(String configFilePath)
	{
		this.configFilePath = configFilePath;
	}

	/**
	 * Sets the Config Root.
	 * @param configRoot
	 */
	public void setConfigRoot(String configRoot)
	{
		this.configRoot = configRoot;
	}

	/**
	 * Sets the Mapper Config.
	 * @param mapperConfig
	 */
	public void setMapperConfig(MapperConfig mapperConfig)
	{
		this.mapperConfig = mapperConfig;
	}

	/**
	 * Sets the Mapper Type List.
	 * @param mapperTypeList
	 */
	public void setMapperTypeList(List<MapperType> mapperTypeList)
	{
		this.mapperTypeList = mapperTypeList;
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
	 * Sets the Destination Event Factory.
	 * @param destinationEventFactory
	 */
	public void setDestinationEventFactory(DestinationEventFactory destinationEventFactory)
	{
		this.destinationEventFactory = destinationEventFactory;
	}
}
