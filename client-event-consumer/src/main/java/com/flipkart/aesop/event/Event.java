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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.databus.core.DbusOpcode;

/**
 * Event class. Every event should implement this class or extend the Abstract Implementation of this class.
 * @author Prakhar Jain
 * @see AbstractEvent
 */
public interface Event
{
	/**
	 * Get the Columns Map.
	 * @return Field Map
	 */
	public Map<String, Object> getFieldMapPair();

	/**
	 * Get the column value for the column name.
	 * @param key
	 * @return Column Value
	 */
	public Object get(String key);

	/**
	 * Checks if key is composite or not.
	 * @return {@link Boolean}
	 */
	public boolean isCompositeKey();

	/**
	 * Gets the primary key set.
	 * @return
	 */
	public Set<String> getPrimaryKeySet();

	/**
	 * Gets the entity name of the event.
	 * @return Entity
	 */
	public String getEntityName();

	/**
	 * Gets the namespace name.
	 * @return Namepspace
	 */
	public String getNamespaceName();

	/**
	 * Gets the Event type of the event.
	 * @return {@link DbusOpcode#DELETE} or {@link DbusOpcode#UPSERT}
	 */
	public DbusOpcode getEventType();

	/**
	 * Gets the values for all the primary column names as {@link List}.
	 * @return {@link List} of Primary Keys
	 */
	public List<Object> getPrimaryKeyValues();
}
