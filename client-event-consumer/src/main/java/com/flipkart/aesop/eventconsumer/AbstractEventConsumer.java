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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.DestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.EventFactory;
import com.flipkart.aesop.event.implementation.SourceEvent;
import com.flipkart.aesop.eventconsumer.implementation.DefaultEventConsumerImpl;
import com.flipkart.aesop.mapper.Mapper;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;

/**
 * Extend this class to implement your Event Consumer, or use {@link DefaultEventConsumerImpl}.
 * @author Jagadeesh Huliyar
 * @author Prakhar Jain
 * @see DefaultEventConsumerImpl
 */
public abstract class AbstractEventConsumer extends AbstractDatabusCombinedConsumer
{
	/** Logger for this class */
	public static final Logger LOGGER = LogFactory.getLogger(AbstractEventConsumer.class);

	/** Factory which generates {@link SourceEvent} using {@link DbusEvent} and {@link DbusEventDecoder}. */
	protected EventFactory sourceEventFactory;
	/** Maps {@link SourceEvent} to {@link List of DestinationEvent}. */
	protected Mapper mapper;
	/** Map from {@link DbusOpcode} to the corresponding {@link DestinationStoreOperation} implementation. */
	protected Map<DbusOpcode, ? extends DestinationStoreOperation> destinationOperationsMap;
	/** Set of Group Ids which the event consumer should consume. */
	protected Set<Integer> destinationGroupSet;
	/**
	 * Total Destination Groups. This is used in case Group Id is not specified in the HOCON-config and hence, group id
	 * is figured out using the {@link SourceEvent} and this number.
	 */
	protected Integer totalDestinationGroups;

	/**
	 * Abstract Builder for AbstractEventConsumer.
	 * @param <T> Event Consumer Implementation class
	 */
	public abstract static class Builder<T>
	{
		protected EventFactory sourceEventFactory;
		protected Mapper mapper;
		protected Map<DbusOpcode, ? extends DestinationStoreOperation> destinationOperationsMap;

		protected Set<Integer> destinationGroupSet = new HashSet<Integer>();
		protected Integer totalDestinationGroups = 1;

		public Builder(EventFactory sourceEventFactory, Mapper mapper,
		        Map<DbusOpcode, ? extends DestinationStoreOperation> destinationOperationsMap)
		{
			this.sourceEventFactory = sourceEventFactory;
			this.mapper = mapper;
			this.destinationOperationsMap = destinationOperationsMap;
		}

		public EventFactory getSourceEventFactory()
		{
			return sourceEventFactory;
		}

		public Mapper getMapper()
		{
			return mapper;
		}

		public Map<DbusOpcode, ? extends DestinationStoreOperation> getDestinationOperationsMap()
		{
			return destinationOperationsMap;
		}

		public Set<Integer> getDestinationGroupSet()
		{
			return destinationGroupSet;
		}

		public Integer getTotalDestinationGroups()
		{
			return totalDestinationGroups;
		}

		public Builder<T> destinationGroupSet(Set<Integer> destinationGroupSet)
		{
			this.destinationGroupSet = destinationGroupSet;
			return this;
		}

		public Builder<T> totalDestinationGroups(Integer totalDestinationGroups)
		{
			this.totalDestinationGroups = totalDestinationGroups;
			return this;
		}

		public abstract T build();
	}

	/**
	 * Overridden superclass method. Returns the result of calling
	 * {@link DefaultEventConsumer#processEvent(DbusEvent, DbusEventDecoder)}
	 * @see com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer#onDataEvent(com.linkedin.databus.core.DbusEvent,
	 *      com.linkedin.databus.client.pub.DbusEventDecoder)
	 */
	public ConsumerCallbackResult onDataEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	{
		return processEvent(event, eventDecoder);
	}

	/**
	 * Overridden superclass method. Returns the result of calling
	 * {@link DefaultEventConsumer#processEvent(DbusEvent, DbusEventDecoder)}
	 * @see com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer#onBootstrapEvent(com.linkedin.databus.core.DbusEvent,
	 *      com.linkedin.databus.client.pub.DbusEventDecoder)
	 */
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	{
		return processEvent(event, eventDecoder);
	}

	/**
	 * Helper method that prints out the attributes of the change event.
	 * @param event the Databus change event
	 * @param eventDecoder the Event decoder
	 * @return {@link ConsumerCallbackResult#SUCCESS} if successful and {@link ConsumerCallbackResult#ERROR} in case of
	 *         exceptions/errors
	 */
	private ConsumerCallbackResult processEvent(DbusEvent dbusEvent, DbusEventDecoder eventDecoder)
	{

		LOGGER.debug("Source Id is " + dbusEvent.getSourceId());
		AbstractEvent event = null;
		try
		{
			event = decodeSourceEvent(dbusEvent, eventDecoder);
			LOGGER.info("Event : " + event.toString()); // Log Properly

		}
		catch (DatabusException ex)
		{
			LOGGER.error("error in consuming events", ex);
			return ConsumerCallbackResult.ERROR;
		}
		return processSourceEvent(event);
	}

	/**
	 * Decodes the {@link DbusEvent} to {@link SourceEvent} using {@link DbusEventDecoder}
	 * @param event
	 * @param eventDecoder
	 * @return Source Event
	 * @throws DatabusException
	 */
	public abstract AbstractEvent decodeSourceEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	        throws DatabusException;

	/**
	 * Processes source event. Ideal implementation, firstly, maps the {@link SourceEvent} to {@link List of
	 * DestinationEvent} and then passes each {@code DestinationEvent} to the corresponding
	 * {@link DestinationStoreOperation}.
	 * @param event Source Event
	 * @return {@link ConsumerCallbackResult#SUCCESS} if successful and {@link ConsumerCallbackResult#ERROR} in case of
	 *         exceptions/errors
	 */
	public abstract ConsumerCallbackResult processSourceEvent(AbstractEvent event);

	/**
	 * Gets the group Id set to be processed for this event consumer.
	 * @return Set of Group Ids
	 */
	public Set<Integer> getDestinationGroupSet()
	{
		return destinationGroupSet;
	}

	/**
	 * Sets the group Id set to be processed for this event consumer.
	 * @param totalDestinationGroups
	 */
	public void setTotalDestinationGroups(Integer totalDestinationGroups)
	{
		this.totalDestinationGroups = totalDestinationGroups;
	}
}
