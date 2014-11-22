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

package com.flipkart.aesop.eventconsumer.implementation;

import java.util.List;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flipkart.aesop.destinationoperation.DestinationStoreOperation;
import com.flipkart.aesop.destinationoperation.implementation.DefaultDeleteDataLayer;
import com.flipkart.aesop.destinationoperation.implementation.DefaultUpsertDataLayer;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.EventFactory;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.flipkart.aesop.mapper.Mapper;
import com.flipkart.aesop.mapper.implementation.DefaultMapperImpl;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;

/**
 * Default Implementation of {@link EventConsumer}.
 * Different {@link Mapper}, {@link DestinationStoreOperation} can be implemented and plugged in. Though, default
 * implementations are provided.
 * <p>
 * @author Prakhar Jain
 * @see DefaultMapperImpl
 * @see DefaultUpsertDataLayer
 * @see DefaultDeleteDataLayer
 */
public class DefaultEventConsumerImpl extends AbstractEventConsumer
{
	/** Logger for this class. */
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultEventConsumerImpl.class);

	/** Object Builder for this class. */
	public static class Builder extends AbstractEventConsumer.Builder<DefaultEventConsumerImpl>
	{
		public Builder(EventFactory sourceEventFactory, Mapper mapper,
		        Map<DbusOpcode, ? extends DestinationStoreOperation> destinationOperationsMap)
		{
			super(sourceEventFactory, mapper, destinationOperationsMap);
		}

		@Override
		public DefaultEventConsumerImpl build()
		{
			return new DefaultEventConsumerImpl(this);
		}
	}

	/**
	 * Private Constructor that uses {@link Builder} instance.
	 * @param builder
	 */
	private DefaultEventConsumerImpl(Builder builder)
	{
		this.mapper = builder.getMapper();
		this.sourceEventFactory = builder.getSourceEventFactory();
		this.destinationOperationsMap = builder.getDestinationOperationsMap();
		this.destinationGroupSet = builder.getDestinationGroupSet();
		this.totalDestinationGroups = builder.getTotalDestinationGroups();
	}

	@Override
	public AbstractEvent decodeSourceEvent(DbusEvent dbusEvent, DbusEventDecoder eventDecoder) throws DatabusException
	{
		AbstractEvent event = sourceEventFactory.createEvent(dbusEvent, eventDecoder);
		return event;
	}

	@Override
	public ConsumerCallbackResult processSourceEvent(AbstractEvent event)
	{
		List<AbstractEvent> destinationEventList =
		        mapper.mapSourceEventToDestinationEvent(event, destinationGroupSet, totalDestinationGroups);

		for (AbstractEvent destinationEvent : destinationEventList)
		{
			DestinationStoreOperation destinationStoreOperation =
			        destinationOperationsMap.get(destinationEvent.getEventType());

			try
			{
				destinationStoreOperation.execute(destinationEvent);
			}
			catch (OperationNotSupportedException e)
			{
				LOGGER.error("Operation Not Supported Exception occured while executing Destination Store Operation.",
				        e);
			}
		}
		return ConsumerCallbackResult.SUCCESS;
	}
}
