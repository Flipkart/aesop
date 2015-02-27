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

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.EventFactory;
import com.flipkart.aesop.event.implementation.SourceEvent;
import com.flipkart.aesop.eventconsumer.implementation.DefaultEventConsumerImpl;
import com.flipkart.aesop.mapper.Mapper;
import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /** Map from {@link com.linkedin.databus.core.DbusOpcode} to the corresponding {@link DestinationEventProcessor} implementation. */
    protected Map<DbusOpcode, ? extends DestinationEventProcessor> destinationProcessorMap;

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
        protected Map<DbusOpcode, ? extends DestinationEventProcessor> destinationProcessorMap;
		protected Set<Integer> destinationGroupSet = new HashSet<Integer>();
		protected Integer totalDestinationGroups = 1;

		public Builder(EventFactory sourceEventFactory, Mapper mapper, Map<DbusOpcode, ? extends DestinationEventProcessor> destinationProcessorMap)
		{
			this.sourceEventFactory = sourceEventFactory;
			this.mapper = mapper;
			this.destinationProcessorMap = destinationProcessorMap;
		}

		public EventFactory getSourceEventFactory()
		{
			return sourceEventFactory;
		}

		public Mapper getMapper()
		{
			return mapper;
		}

        public Map<DbusOpcode, ? extends DestinationEventProcessor> getDestinationProcessorMap()
		{
			return destinationProcessorMap;
		}

		public Set<Integer> getDestinationGroupSet()
		{
			return destinationGroupSet;
		}

		public Integer getTotalDestinationGroups()
		{
			return totalDestinationGroups;
		}

		public Builder<T> withDestinationGroupSet(Set<Integer> destinationGroupSet)
		{
			this.destinationGroupSet = destinationGroupSet;
			return this;
		}

		public Builder<T> withTotalDestinationGroups(Integer totalDestinationGroups)
		{
			this.totalDestinationGroups = totalDestinationGroups;
			return this;
		}

		public abstract T build();
    }

	/**
	 * Overridden superclass method. Returns the result of calling
	 * {@link AbstractEventConsumer#processEvent(DbusEvent, DbusEventDecoder)}
	 * @see com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer#onDataEvent(com.linkedin.databus.core.DbusEvent,
	 *      com.linkedin.databus.client.pub.DbusEventDecoder)
	 */
	public ConsumerCallbackResult onDataEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	{
		return processEvent(event, eventDecoder);
	}

	/**
	 * Overridden superclass method. Returns the result of calling
	 * {@link AbstractEventConsumer#processEvent(DbusEvent, DbusEventDecoder)}
	 * @see com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer#onBootstrapEvent(com.linkedin.databus.core.DbusEvent,
	 *      com.linkedin.databus.client.pub.DbusEventDecoder)
	 */
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	{
		return processEvent(event, eventDecoder);
	}

	/**
	 * Helper method that prints out the attributes of the change event.
	 * @param dbusEvent the Databus change event
	 * @param eventDecoder the Event decoder
	 * @return {@link ConsumerCallbackResult#SUCCESS} if successful and {@link ConsumerCallbackResult#ERROR} in case of
	 *         exceptions/errors
	 */
	private ConsumerCallbackResult processEvent(DbusEvent dbusEvent, DbusEventDecoder eventDecoder)
	{

		LOGGER.debug("Source Id is " + dbusEvent.getSourceId());
		AbstractEvent event;
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
	 * @param event :   databus event
	 * @param eventDecoder  : databus Event Decoder
	 * @return Source Event
	 * @throws DatabusException : dataBus Exception thrown
	 */
	public abstract AbstractEvent decodeSourceEvent(DbusEvent event, DbusEventDecoder eventDecoder)
	        throws DatabusException;

	/**
	 * Processes source event. Ideal implementation, firstly, maps the {@link SourceEvent} to {@link List of
	 * DestinationEvent} and then passes each {@code DestinationEvent} to the corresponding
	 * {@link DestinationEventProcessor}.
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
