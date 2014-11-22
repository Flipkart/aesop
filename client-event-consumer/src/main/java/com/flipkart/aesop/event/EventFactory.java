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

package com.flipkart.aesop.event;

import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;

import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;

/**
 * Event Factory Interface to be implemented by the factory classes which generate events, or extend the Abstract
 * implementation of this interface.
 * @author Prakhar Jain
 * @see AbstractEventFactory
 * @see AbstractEvent
 */
public interface EventFactory
{
	/**
	 * Generates {@link AbstractEvent} from {@link DbusEvent} using {@link DbusEventDecoder}.
	 * @param event
	 * @param eventDecoder
	 * @return {@link AbstractEvent}
	 * @throws DatabusException
	 */
	public AbstractEvent createEvent(DbusEvent event, DbusEventDecoder eventDecoder) throws DatabusException;

	/**
	 * Generates {@link AbstractEvent} using {@link Schema}.
	 * @param schema
	 * @param fieldMap
	 * @param eventType
	 * @return {@link AbstractEvent}
	 * @throws DatabusException
	 */
	public AbstractEvent createEvent(Schema schema, Map<String, Object> fieldMap, DbusOpcode eventType)
	        throws DatabusException;

	/**
	 * Generates {@link AbstractEvent} using all the mandatory fields required in {@code Event}.
	 * @param fieldsMap
	 * @param primaryFieldsSet
	 * @param entityName
	 * @param namespaceName
	 * @param eventType
	 * @return {@link AbstractEvent}
	 */
	public AbstractEvent createEvent(Map<String, Object> fieldsMap, Set<String> primaryFieldsSet, String entityName,
	        String namespaceName, DbusOpcode eventType);
}
