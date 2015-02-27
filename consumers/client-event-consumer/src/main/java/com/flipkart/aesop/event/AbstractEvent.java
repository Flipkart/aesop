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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.linkedin.databus.core.DbusOpcode;

/**
 * Abstract Class which implements functions of {@link Event} class.
 * @author Prakhar Jain
 */
public abstract class AbstractEvent implements Event
{
	/** Columns Map. */
	protected final Map<String, Object> fieldMap;
	/** Primary Key Fields. */
	protected final Set<String> primaryKeysSet;
	/** Entity of the Event. */
	protected final String entityName;
	/** Namespace of the Event. */
	protected final String namespaceName;
	/** Event Type. */
	protected final DbusOpcode eventType;

	/**
	 * Constructs the basic event using mandatory fields.
	 * @param fieldsMap
	 * @param primaryKeysSet
	 * @param entityName
	 * @param namespaceName
	 * @param eventType
	 */
	public AbstractEvent(Map<String, Object> fieldsMap, Set<String> primaryKeysSet, String entityName,
	        String namespaceName, DbusOpcode eventType)
	{
		this.fieldMap = fieldsMap;
		this.primaryKeysSet = primaryKeysSet;
		this.entityName = entityName;
		this.namespaceName = namespaceName;
		this.eventType = eventType;
	}

	public Map<String, Object> getFieldMapPair()
	{
		return fieldMap;
	}

	public Object get(String key)
	{
		return fieldMap.get(key);
	}

	public boolean isCompositeKey()
	{
		return primaryKeysSet.size() > 1;
	}

	public Set<String> getPrimaryKeySet()
	{
		return primaryKeysSet;
	}

	public String getEntityName()
	{
		return entityName;
	}

	public String getNamespaceName()
	{
		return namespaceName;
	}

	public DbusOpcode getEventType()
	{
		return eventType;
	}

	public List<Object> getPrimaryKeyValues()
	{
		List<Object> primaryKeyValues = new ArrayList<Object>();
		for (String primaryKey : primaryKeysSet)
		{
			primaryKeyValues.add(fieldMap.get(primaryKey));
		}
		return primaryKeyValues;
	}

	@Override
	public String toString()
	{
		return "AbstractEvent [fieldsMap=" + fieldMap + ", primaryKeysSet=" + primaryKeysSet + ", entityName="
		        + entityName + ", namespaceName=" + namespaceName + ", eventType=" + eventType + "]";
	}
}
