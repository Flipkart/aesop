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

import com.flipkart.aesop.event.AbstractEventFactory;
import com.linkedin.databus.core.DbusOpcode;

/**
 * Destination Event Factory.
 * @author Prakhar Jain
 */
public class DestinationEventFactory extends AbstractEventFactory<DestinationEvent>
{
	@Override
	protected DestinationEvent createEventInstance(Map<String, Object> fieldsMap, Set<String> primaryKeysSet,
	        String entityName, String namespaceName, DbusOpcode eventType, Map<String, Object> rowChangeMap)
	{
		return new DestinationEvent(fieldsMap, primaryKeysSet, entityName, namespaceName, eventType, rowChangeMap);
	}
}
