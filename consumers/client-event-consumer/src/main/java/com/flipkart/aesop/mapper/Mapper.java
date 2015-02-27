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

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.implementation.DestinationEvent;
import com.flipkart.aesop.event.implementation.SourceEvent;

import java.util.List;
import java.util.Set;

/**
 * Mapper Interface to be implemented by the {@link SourceEvent} to {@link DestinationEvent} mappers.
 * @author Prakhar Jain
 */
public interface Mapper
{
	/**
	 * Maps {@link SourceEvent} to {@link List} of {@link DestinationEvent} and filters based on group Id set.
	 * @param sourceEvent
	 * @param destinationGroupSet
	 * @param totalDestinationGroups
	 * @return {@link List} of {@link DestinationEvent}
	 */
	List<AbstractEvent> mapSourceEventToDestinationEvent(AbstractEvent sourceEvent, Set<Integer> destinationGroupSet,
	        int totalDestinationGroups);

	/**
	 * Maps {@link SourceEvent} to {@link List} of {@link DestinationEvent} assuming group Id set contains group Id 1
	 * and total number of destination group Ids is 1.
	 * @param sourceEvent
	 * @return {@link List} of {@link DestinationEvent}
	 */
	List<AbstractEvent> mapSourceEventToDestinationEvent(AbstractEvent sourceEvent);
}
