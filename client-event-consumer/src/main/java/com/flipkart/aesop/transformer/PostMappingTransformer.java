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
package com.flipkart.aesop.transformer;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.event.implementation.DestinationEvent;

/**
 * PreMappingTransformer Interface to be implemented by the {@link com.flipkart.aesop.eventconsumer.AbstractEventConsumer}
 * This can be used to transform destination events {@link DestinationEvent} in
 * the post mapping phase
 *
 * Use-cases :
 *  Users may want to add or remove some  fields statically or by calling an external API.
 *  As an example
 *  adding date fields with some default values to your destination events.
 *
 */
public interface PostMappingTransformer
{
    /**
     * This will be called to transform events in the post mapping phase
	 * @param event The event
	 * @return {@link com.flipkart.aesop.event.implementation.DestinationEvent}
	 */
	AbstractEvent transform(AbstractEvent event);
}
