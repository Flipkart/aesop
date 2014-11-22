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

package com.flipkart.aesop.eventconsumer;

import java.util.Map;
import java.util.Set;

import com.flipkart.aesop.destinationoperation.DestinationStoreOperation;
import com.flipkart.aesop.event.implementation.SourceEventFactory;
import com.flipkart.aesop.eventconsumer.implementation.DefaultEventConsumerImpl;
import com.flipkart.aesop.mapper.Mapper;
import com.linkedin.databus.core.DbusOpcode;

/**
 * Abstract Factory to be extended by Factory classes which generate Event Consumers.
 * @author Prakhar Jain
 * @param <T> Event Consumer Implementation
 * @see DefaultEventConsumerImpl
 */
public abstract class EventConsumerFactoryBean<T extends AbstractEventConsumer>
{
	protected Mapper mapper;
	protected Map<DbusOpcode, ? extends DestinationStoreOperation> destStoreOperationsMap;
	protected SourceEventFactory sourceEventFactory;
	protected Set<Integer> destinationGroupSet;
	protected Integer totalDestinationGroups;

	/**
	 * Get Event Consumer Object
	 * @return Event Consumer Object
	 */
	public T getObject()
	{
		return getEventConsumerObject();
	}

	/**
	 * Returns actual Event consumer Implementation instance.
	 * @return Event consumer Object
	 */
	public abstract T getEventConsumerObject();

	/**
	 * Set Mapper to be used by the Event Consumer.
	 * @param mapper
	 */
	public void setMapper(Mapper mapper)
	{
		this.mapper = mapper;
	}

	/**
	 * Sets Destination Store Map to be used by the Event Consumer.
	 * @param destStoreOperationsMap
	 */
	public void setDestStoreOperationsMap(Map<DbusOpcode, DestinationStoreOperation> destStoreOperationsMap)
	{
		this.destStoreOperationsMap = destStoreOperationsMap;
	}

	/**
	 * Sets the Source Event Factory to be used by the Event Consumer.
	 * @param sourceEventFactory
	 */
	public void setSourceEventFactory(SourceEventFactory sourceEventFactory)
	{
		this.sourceEventFactory = sourceEventFactory;
	}

	/**
	 * Sets the Destination Group Set to be used by the Event Consumer.
	 * @param destinationGroupSet
	 */
	public void setDestinationGroupSet(Set<Integer> destinationGroupSet)
	{
		this.destinationGroupSet = destinationGroupSet;
	}

	/**
	 * Sets the Total Number of Destination Groups to be used by the Event Consumer.
	 * @param totalDestinationGroups
	 */
	public void setTotalDestinationGroups(Integer totalDestinationGroups)
	{
		this.totalDestinationGroups = totalDestinationGroups;
	}
}
