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

package com.flipkart.aesop.destinationoperation.implementation;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Delete Destination Operation Layer which logs the delete events.
 * @author Prakhar Jain
 */
public class DefaultDeleteDataLayer extends DeleteDestinationStoreProcessor
{
    /** Logger for this class. */
    public final static Logger LOGGER = LoggerFactory.getLogger(DefaultDeleteDataLayer.class);

    @Override
    protected ConsumerCallbackResult delete(AbstractEvent event)
    {
        LOGGER.info("Delete Event Received " + event.toString() + " by Default Delete Data Layer");
        return ConsumerCallbackResult.SUCCESS;
    }

}
