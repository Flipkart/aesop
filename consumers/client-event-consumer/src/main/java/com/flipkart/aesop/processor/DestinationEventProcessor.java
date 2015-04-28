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
package com.flipkart.aesop.processor;

import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

import javax.naming.OperationNotSupportedException;

/**
 * DestinationEventProcessor interface needs to be implemented by the {@link com.flipkart.aesop.eventconsumer.AbstractEventConsumer}
 * for processing destination Events.
 * This provides the users the flexibility to process destination events as per their work-flow.
 * Example Use-case :
 * Users can write their own destinationEvent processor which adapts the {@link AbstractEvent} to some other POJO and send  the
 * adapted POJO to the next layer.
 *
 */
public interface DestinationEventProcessor
{
    /**
     *  method to process destination Event
     * @param destinationEvent of type {@link AbstractEvent}
     */
     public ConsumerCallbackResult processDestinationEvent(AbstractEvent destinationEvent) throws OperationNotSupportedException;
}
