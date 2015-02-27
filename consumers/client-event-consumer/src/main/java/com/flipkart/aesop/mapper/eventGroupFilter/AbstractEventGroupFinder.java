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

package com.flipkart.aesop.mapper.eventGroupFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.mapper.eventGroupFilter.implementation.DefaultEventGroupFinderImpl;

/**
 * Abstract Implementation of the {@link EventGroupFinder}. Preferably, extend this class to extend thiS class to
 * implement your own {@link EventGroupFinder}.
 * @author Prakhar Jain
 * @see DefaultEventGroupFinderImpl
 */
public abstract class AbstractEventGroupFinder implements EventGroupFinder
{
	/** Namespace Entity Separator. */
	public String namespaceEntitySeparator = "__";
	/** Default Group Id. */
	public Integer defaultGroup = 1;

	public List<AbstractEvent> filterEventsByGroup(List<AbstractEvent> eventList, Integer destinationGroupNo,
	        Integer totalDestinationGroups)
	{
		List<AbstractEvent> filteredEventList = new ArrayList<AbstractEvent>();
		for (AbstractEvent abstractEvent : eventList)
		{
			String namespaceEntity =
			        abstractEvent.getNamespaceName() + namespaceEntitySeparator + abstractEvent.getEntityName();
			if (getEventGroupNo(namespaceEntity, totalDestinationGroups) == destinationGroupNo)
			{
				filteredEventList.add(abstractEvent);
			}
		}
		return filteredEventList;
	}

	public AbstractEvent filterEventByGroup(AbstractEvent event, Integer destinationGroupNo,
	        Integer totalDestinationGroups)
	{
		List<AbstractEvent> filteredEvent =
		        filterEventsByGroup(Arrays.asList(event), destinationGroupNo, totalDestinationGroups);

		if (filteredEvent == null || filteredEvent.size() == 0)
		{
			return null;
		}
		return filteredEvent.get(0);
	}

	public abstract Integer getEventGroupNo(String namespaceEntity, Integer totalDestinationGroups);

	public Integer getEventGroupNo(String namespace, String entity, Integer totalDestinationGroups)
	{
		String namespaceEntity = namespace + namespaceEntitySeparator + entity;

		return getEventGroupNo(namespaceEntity, totalDestinationGroups);
	}
}
