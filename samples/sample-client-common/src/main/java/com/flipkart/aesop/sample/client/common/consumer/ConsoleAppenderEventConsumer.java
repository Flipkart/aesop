
package com.flipkart.aesop.sample.client.common.consumer;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.sample.client.common.events.MysqlBinLogEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;

/**
 * yogesh.dahiya
 */

public class ConsoleAppenderEventConsumer extends AbstractMySqlEventConsumer
{
    public static final Logger LOGGER = LogFactory.getLogger(ConsoleAppenderEventConsumer.class);

    @Override
    public ConsumerCallbackResult processEvent(MysqlBinLogEvent mysqlBinLogEvent)
    {
        LOGGER.debug("Event : " + mysqlBinLogEvent.toString());
        return ConsumerCallbackResult.SUCCESS;
    }
}
