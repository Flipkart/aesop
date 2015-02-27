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

import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.flipkart.aesop.event.implementation.SourceEventFactory;
import com.flipkart.aesop.eventconsumer.implementation.DefaultEventConsumerImpl;
import com.flipkart.aesop.mapper.Mapper;
import com.flipkart.aesop.transformer.PostMappingTransformer;
import com.flipkart.aesop.transformer.PreMappingTransformer;
import com.linkedin.databus.core.DbusOpcode;

import java.util.Map;
import java.util.Set;

/**
 * Abstract Factory to be extended by Factory classes which generate Event Consumers.
 * @author Prakhar Jain
 * @param <T> Event Consumer Implementation
 * @see DefaultEventConsumerImpl
 */
public abstract class EventConsumerFactoryBean<T extends AbstractEventConsumer>
{
    protected Mapper mapper;
    protected Map<DbusOpcode, ? extends DestinationEventProcessor> destinationProcessorMap;
    protected SourceEventFactory sourceEventFactory;
    protected Set<Integer> destinationGroupSet;
    protected Integer totalDestinationGroups;
    protected PreMappingTransformer preMappingTransformer;
    protected PostMappingTransformer postMappingTransformer;

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
     * @param mapper mapper
     */
    public void setMapper(Mapper mapper)
    {
        this.mapper = mapper;
    }

    /**
     * Sets the Source Event Factory to be used by the Event Consumer.
     * @param sourceEventFactory sourceEventFactory
     */
    public void setSourceEventFactory(SourceEventFactory sourceEventFactory)
    {
        this.sourceEventFactory = sourceEventFactory;
    }

    /**
     * Sets the Destination Group Set to be used by the Event Consumer.
     * @param destinationGroupSet destinationGroupSet
     */
    public void setDestinationGroupSet(Set<Integer> destinationGroupSet)
    {
        this.destinationGroupSet = destinationGroupSet;
    }

    /**
     * Sets the Total Number of Destination Groups to be used by the Event Consumer.
     * @param totalDestinationGroups totalDestinationGroups
     */
    public void setTotalDestinationGroups(Integer totalDestinationGroups)
    {
        this.totalDestinationGroups = totalDestinationGroups;
    }

    /**
     * Sets the preMapping PreMappingTransformer to be used by the Event Consumer.
     * @param preMappingTransformer  preMappingPreMappingTransformer
     */
    public void setPreMappingTransformer(PreMappingTransformer preMappingTransformer)
    {
        this.preMappingTransformer = preMappingTransformer;
    }

    /**
     * Sets the postMappingPreMappingTransformer to be used by the Event Consumer.
     * @param postMappingTransformer  postMappingPreMappingTransformer
     */
    public void setPostMappingTransformer(PostMappingTransformer postMappingTransformer)
    {
        this.postMappingTransformer = postMappingTransformer;
    }

    /**
     * Sets the DestinationProcessorMap to be used by the Event Consumer.
     * @param destinationProcessorMap destinationProcessorMap
     */
    public void setDestinationProcessorMap(Map<DbusOpcode, ? extends DestinationEventProcessor> destinationProcessorMap)
    {
        this.destinationProcessorMap = destinationProcessorMap;
    }
}
