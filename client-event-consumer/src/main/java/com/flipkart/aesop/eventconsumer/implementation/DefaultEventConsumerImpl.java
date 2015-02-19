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

import com.flipkart.aesop.destinationoperation.implementation.DefaultDeleteDataLayer;
import com.flipkart.aesop.destinationoperation.implementation.DefaultUpsertDataLayer;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.EventFactory;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.flipkart.aesop.mapper.Mapper;
import com.flipkart.aesop.mapper.implementation.DefaultMapperImpl;
import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.flipkart.aesop.transformer.PostMappingTransformer;
import com.flipkart.aesop.transformer.PreMappingTransformer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;

import javax.naming.OperationNotSupportedException;
import java.util.List;
import java.util.Map;

/**
 * Default Implementation of {@link AbstractEventConsumer}.
 * Different {@link Mapper}, {@link DestinationEventProcessor} can be implemented and plugged in. Though, default
 * implementations are provided.
 * <p>
 * @author Prakhar Jain
 * @see DefaultMapperImpl
 * @see DefaultUpsertDataLayer
 * @see DefaultDeleteDataLayer
 */
public class DefaultEventConsumerImpl extends AbstractEventConsumer
{
    /* PreMappingTransformer instance for pre mapping transformation */
    private PreMappingTransformer preMappingPreMappingTransformer;

    /* PreMappingTransformer instance for post mapping transformation */
    private PostMappingTransformer postMappingPreMappingTransformer;

    /** Object Builder for this class. */
    public static class Builder extends AbstractEventConsumer.Builder<DefaultEventConsumerImpl>
    {
        private PreMappingTransformer preMappingPreMappingTransformer;
        private PostMappingTransformer postMappingPreMappingTransformer;

        public Builder(EventFactory sourceEventFactory, Mapper mapper,
                       Map<DbusOpcode, ? extends DestinationEventProcessor> destinationEventProcessorMap)
        {
            super(sourceEventFactory, mapper, destinationEventProcessorMap);
        }

        public PreMappingTransformer getPreMappingPreMappingTransformer()
        {
            return preMappingPreMappingTransformer;
        }

        public PostMappingTransformer getPostMappingPreMappingTransformer()
        {
            return postMappingPreMappingTransformer;
        }

        public Builder withPreMappingTransformer(PreMappingTransformer preMappingPreMappingTransformer)
        {
            this.preMappingPreMappingTransformer = preMappingPreMappingTransformer;
            return this;
        }

        public Builder withPostMappingTransformer(PostMappingTransformer postMappingPreMappingTransformer)
        {
            this.postMappingPreMappingTransformer = postMappingPreMappingTransformer;
            return this;
        }

        @Override
        public DefaultEventConsumerImpl build()
        {
            return new DefaultEventConsumerImpl(this);
        }
    }

    /**
     * Private Constructor that uses {@link Builder} instance.
     * @param builder : Builder object to build EventConsumer
     */
    private DefaultEventConsumerImpl(Builder builder)
    {
        this.mapper = builder.getMapper();
        this.sourceEventFactory = builder.getSourceEventFactory();
        this.destinationGroupSet = builder.getDestinationGroupSet();
        this.destinationProcessorMap = builder.getDestinationProcessorMap();
        this.totalDestinationGroups = builder.getTotalDestinationGroups();
        this.preMappingPreMappingTransformer = builder.getPreMappingPreMappingTransformer();
        this.postMappingPreMappingTransformer = builder.getPostMappingPreMappingTransformer();
    }

    @Override
    public AbstractEvent decodeSourceEvent(DbusEvent dbusEvent, DbusEventDecoder eventDecoder) throws DatabusException
    {
        return  sourceEventFactory.createEvent(dbusEvent, eventDecoder);
    }

    @Override
    public ConsumerCallbackResult processSourceEvent(AbstractEvent sourceEvent)
    {
        /* Pre Mapping of Source Event , calling PreMappingTransformer for the source event */
        AbstractEvent sourceEventAfterTransformation = preMappingPreMappingTransformer == null ? sourceEvent :
                preMappingPreMappingTransformer.transform(sourceEvent);

        List<AbstractEvent> destinationEventList =
                mapper.mapSourceEventToDestinationEvent(sourceEventAfterTransformation, destinationGroupSet, totalDestinationGroups);

        for (AbstractEvent destinationEvent : destinationEventList)
        {
            /* Post Mapping of Source Event , calling PreMappingTransformer for the destination event */
            AbstractEvent destinationEventAfterTransformation = (postMappingPreMappingTransformer == null ?
                    destinationEvent : postMappingPreMappingTransformer.transform(destinationEvent));

            DestinationEventProcessor destinationEventProcessor =
			        destinationProcessorMap.get(destinationEvent.getEventType());

            /* Process Destination Event */
			try
			{
                if(destinationEventProcessor != null) {
				    destinationEventProcessor.processDestinationEvent(destinationEventAfterTransformation);
                }
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
