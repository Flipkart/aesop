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

package com.flipkart.aesop.event.implementation;

import java.util.Map;
import java.util.Set;

import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;

/**
 * Destination Event class. Event to be persisted to data store.
 * @author Prakhar Jain
 * @see AbstractEvent
 * @see SourceEvent
 */
public class DestinationEvent extends AbstractEvent
{
	/**
	 * Destination Event All-fields constructor.
	 * @param fieldsMap
	 * @param primaryKeysSet
	 * @param entityName
	 * @param namespaceName
	 * @param eventType
	 */
	public DestinationEvent(Map<String, Object> fieldsMap, Set<String> primaryKeysSet, String entityName,
	        String namespaceName, DbusOpcode eventType)
	{
		super(fieldsMap, primaryKeysSet, entityName, namespaceName, eventType);
	}
}
