package com.flipkart.aesop.sample.client.processor;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import javax.naming.OperationNotSupportedException;

/**
 * Entity Upsert Data Layer. Persists {@link com.linkedin.databus.core.DbusOpcode#UPSERT} events to Logs.
 * @author arya.ketan
 */
public class ConsoleProcessor implements DestinationEventProcessor
{
    public static final Logger LOGGER = LogFactory.getLogger(ConsoleProcessor.class);
    public ConsumerCallbackResult processDestinationEvent(AbstractEvent abstractEvent) throws OperationNotSupportedException
    {
        LOGGER.info("CONSOLE_PROCESSOR:Event Type: " + abstractEvent.getEventType().toString() + " Event : " + abstractEvent.toString());
        return ConsumerCallbackResult.SUCCESS;
    }
}
