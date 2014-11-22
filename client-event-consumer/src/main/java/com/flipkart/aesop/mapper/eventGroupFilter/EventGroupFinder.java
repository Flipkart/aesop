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

import java.util.List;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.Event;
import com.flipkart.aesop.mapper.eventGroupFilter.implementation.DefaultEventGroupFinderImpl;

/**
 * Interface for {@link EventGroupFinder} which calculates the Group Id of an {@link Event}, or filters the events as
 * per the calculated Group Id.
 * @author Prakhar Jain
 * @see AbstractEventGroupFinder
 * @see DefaultEventGroupFinderImpl
 */
public interface EventGroupFinder
{
	/**
	 * Returns events with group Id as {@code destinationGroupNo}. Group Id is calculated using {@code event} and
	 * {@code totalDestinationGroups}.
	 * @param eventList
	 * @param destinationGroupNo
	 * @param totalDestinationGroups
	 * @return {@link List} of {@link AbstractEvent}
	 */
	List<AbstractEvent> filterEventsByGroup(List<AbstractEvent> eventList, Integer destinationGroupNo,
	        Integer totalDestinationGroups);

	/**
	 * Returns event with group Id as {@code destinationGroupNo}. Group Id is calculated using {@code event} and
	 * {@code totalDestinationGroups}. Returns either 0 or 1 event.
	 * @param eventList
	 * @param destinationGroupNo
	 * @param totalDestinationGroups
	 * @return {@link AbstractEvent} or {@code null}
	 */
	AbstractEvent filterEventByGroup(AbstractEvent eventList, Integer destinationGroupNo, Integer totalDestinationGroups);

	/**
	 * Calculates Group Id using {@code namespaceEntity} and {@code totalDestinationGroups}.
	 * @param namespaceEntity
	 * @param totalDestinationGroups
	 * @return Group Id
	 */
	Integer getEventGroupNo(String namespaceEntity, Integer totalDestinationGroups);

	/**
	 * Calculates Group Id using {@code namespace}, {@code entity} and {@code totalDestinationGroups}.
	 * @param namespace
	 * @param entity
	 * @param totalDestinationGroups
	 * @return Group Id
	 */
	Integer getEventGroupNo(String namespace, String entity, Integer totalDestinationGroups);

}
