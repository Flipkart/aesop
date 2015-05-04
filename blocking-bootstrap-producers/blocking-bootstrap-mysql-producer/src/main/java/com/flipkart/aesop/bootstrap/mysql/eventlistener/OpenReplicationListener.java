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

package com.flipkart.aesop.bootstrap.mysql.eventlistener;

import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.MysqlTransactionManager;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.Map;

/**
 * The <OpenReplicationListener> is a binary log callback implementation of {@link BinlogEventListener}.
 * <a href="https://code.google.com/p/open-replicator/">OpenReplicator</a> provides callback to this implementation.
 * @author nrbafna
 */
public class OpenReplicationListener implements BinlogEventListener
{
    /** Logger for this class */
    private static final Logger LOGGER = LogFactory.getLogger(OpenReplicationListener.class);

    /** MysqlTransactionManager deals with end to end transaction management */
    private final MysqlTransactionManager mysqlTransactionManager;

    /** Holds all registered events and corresponding event processors */
    private Map<Integer, BinLogEventProcessor> processors;

    /** Interested bin log prefix */
    private String binLogPrefix;

    /**
     * Constructor for this class
     * @param mysqlTransactionManager deals with transaction management
     * @param processors contain all registered events and corresponding event processors
     * @param binLogPrefix interested bin log prefix
     */
    public OpenReplicationListener(MysqlTransactionManager mysqlTransactionManager,
                                   Map<Integer, BinLogEventProcessor> processors,
                                   String binLogPrefix)
    {
        this.mysqlTransactionManager = mysqlTransactionManager;
        this.processors = processors;
        this.binLogPrefix = binLogPrefix;
    }

    /**
     * Callback method which gets called whenever any binary log event is generated at the physical resources registered
     * @param event generated BinlogEventV4 event
     * @see com.google.code.or.binlog.BinlogEventListener#onEvents(com.google.code.or.binlog.BinlogEventV4)
     */
    @Override
    public void onEvents(BinlogEventV4 event)
    {
        if (event == null)
        {
            LOGGER.error("Received null event");
            return;
        }
        LOGGER.info("Current SCN:" + event.getHeader().getPosition());
        try
        {
            BinLogEventProcessor processor = processors.get(event.getHeader().getEventType());
            if (processor != null)
            {
                processor.process(event, this);
            }
            else
            {
                LOGGER.warn("Ignoring Unsupported Event! " + event.getHeader().getEventType());
            }
        }
        catch (Exception e)
        {
            LOGGER.error("Exception occurred while processing event " + e);
        }
    }

    /** Getter and Setter methods for this class */
    public Map<Integer, BinLogEventProcessor> getProcessors()
    {
        return processors;
    }

    public void setProcessors(Map<Integer, BinLogEventProcessor> processors)
    {
        this.processors = processors;
    }

    public MysqlTransactionManager getMysqlTransactionManager()
    {
        return mysqlTransactionManager;
    }

    public String getBinLogPrefix()
    {
        return binLogPrefix;
    }

    public void setBinLogPrefix(String binLogPrefix)
    {
        this.binLogPrefix = binLogPrefix;
    }
}
