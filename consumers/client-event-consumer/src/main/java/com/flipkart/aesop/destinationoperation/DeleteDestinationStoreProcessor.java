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

package com.flipkart.aesop.destinationoperation;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;

import javax.naming.OperationNotSupportedException;

/**
 * Delete Destination Operation Processor which processes Delete Events.
 * @author Prakhar Jain
 */
public abstract class DeleteDestinationStoreProcessor implements DestinationEventProcessor
{
	public ConsumerCallbackResult processDestinationEvent(AbstractEvent event) throws OperationNotSupportedException
	{
		if (event.getEventType() == DbusOpcode.DELETE)
		{
			return delete(event);
		}
		else
		{
			return  ConsumerCallbackResult.ERROR_FATAL;
		}
	}

	/**
	 * Delete function to be implemented by the class extending this class.
	 * @param event
	 */
	protected abstract ConsumerCallbackResult delete(AbstractEvent event);
}
