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

import com.linkedin.databus2.core.BackoffTimer;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

/**
 * <code>SourceEventProcessor</code> processes the source event by invoking the registered event consumer.
 * @author nrbafna
 */
public class SourceEventProcessor implements Runnable
{
    public static final Logger LOGGER = LogFactory.getLogger(SourceEventProcessor.class);

    private final AbstractEvent sourceEvent;
    private final AbstractEventConsumer consumer;
    private final BackoffTimer timer;

    public SourceEventProcessor(AbstractEvent sourceEvent, AbstractEventConsumer consumer, BackoffTimer timer)
    {
        this.sourceEvent = sourceEvent;
        this.consumer = consumer;
        this.timer =timer;
    }

    @Override
    public void run()
    {
        LOGGER.info("Processing :" + sourceEvent.getPrimaryKeyValues() + ":" + sourceEvent.getNamespaceName() + ""
                + sourceEvent.getEntityName());
        process();
    }

    private void process() {
        try {
            ConsumerCallbackResult consumerCallbackResult = consumer.processSourceEvent(sourceEvent);
            switch (consumerCallbackResult) {
                case ERROR:
                    /* Since there is an Error. Back Off and Retry After Some time*/
                    this.timer.backoffAndSleep();
                    process();
                    break;
                case SUCCESS:
                case SKIP_CHECKPOINT:
                    break;
                case  ERROR_FATAL:
                    throw new RuntimeException("Fatal Failure for Source Event : " + sourceEvent);
            }
            LOGGER.info(consumerCallbackResult.toString());
        }
        catch (Exception e)
        {
            LOGGER.error("Exception occurred while processing event " + e.getMessage(), e);
            this.timer.backoffAndSleep();
            process();
        }
    }

}
