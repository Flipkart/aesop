/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.aesop.runtime.bootstrap.consumer;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

/**
 * Created by nikhil.bafna on 1/25/15.
 */
public class SourceEventProcessor implements Runnable
{
	public static final Logger LOGGER = LogFactory.getLogger(SourceEventProcessor.class);

	private final AbstractEvent sourceEvent;
	private final AbstractEventConsumer consumer;

	public SourceEventProcessor(AbstractEvent sourceEvent, AbstractEventConsumer consumer)
	{
		this.sourceEvent = sourceEvent;
		this.consumer = consumer;
	}

	public void run()
	{
		try
		{
			LOGGER.info("Processing :" + sourceEvent.getPrimaryKeyValues() + ":" + sourceEvent.getNamespaceName() + "."
			        + sourceEvent.getEntityName());
			ConsumerCallbackResult consumerCallbackResult = consumer.processSourceEvent(sourceEvent);
			LOGGER.info(consumerCallbackResult.toString());
		}
		catch (Exception e)
		{
			LOGGER.error("Exception occurred while processing event " + e.getMessage(), e);
		}
	}

}
