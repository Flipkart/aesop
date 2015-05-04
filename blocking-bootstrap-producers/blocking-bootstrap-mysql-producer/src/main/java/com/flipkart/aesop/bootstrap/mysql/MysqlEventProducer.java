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

package com.flipkart.aesop.bootstrap.mysql;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.BinLogEventProcessor;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.MysqlTransactionManager;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.impl.MysqlTransactionManagerImpl;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.runtime.bootstrap.producer.BlockingEventProducer;
import com.google.code.or.OpenReplicator;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <code>MysqlEventProducer</code> starts OpenReplicator bin log event listener to listen to MySQL events & registers an
 * instance of {@link OpenReplicationListener} to process the events
 * @author nrbafna
 */
public class MysqlEventProducer<T extends AbstractEvent> extends BlockingEventProducer
{
    private static Long startTime = System.nanoTime();

    /** Logger for this class */
    private static final Logger LOGGER = LogFactory.getLogger(MysqlEventProducer.class);
    /** Default Mysql port */
    private static final Integer DEFAULT_MYSQL_PORT = 3306;
    /** Pattern for extracting /3306/mysql-bin out of mysql://or_test%2For_test@localhost:3306/3306/mysql-bin */
    private static final Pattern PATH_PATTERN = Pattern.compile("/([0-9]+)/[a-z|A-Z|0-9|-]+");
    /** Index of server id in pattern match group */
    private static final int SERVER_ID = 1;
    /** Index of bin log prefix in pattern match group */
    private static final int BIN_LOG_PREFIX = 2;

    /** Open Replicator which listens and parses bin log events from Mysql */
    protected OpenReplicator openReplicator;
    /** Mysql transaction manager */
    protected MysqlTransactionManager mysqlTxnManager;
    /** Event Id to Event Processor map */
    protected Map<Integer, BinLogEventProcessor> eventsMap;

    @Override
    public void start(long sinceSCN)
    {
        this.sinceSCN.set(sinceSCN);
        openReplicator = new OpenReplicator();
        String binlogFile;
        try
        {
            String binlogFilePrefix = processUri(new URI(physicalSourceStaticConfig.getUri()));
            int offset = offset(sinceSCN);
            int logid = logid(sinceSCN);
            LOGGER.info("SCN : " + offset + " logid : " + logid);
            binlogFile = String.format("%s.%06d", binlogFilePrefix, logid);
            LOGGER.info("Bin Log File Name : " + binlogFile);
            Map<String, Short> tableUriToSrcIdMap = new HashMap<String, Short>();
            Map<String, String> tableUriToSrcNameMap = new HashMap<String, String>();
            for (LogicalSourceStaticConfig sourceConfig : physicalSourceStaticConfig.getSources())
            {
                tableUriToSrcIdMap.put(sourceConfig.getUri().toLowerCase(), sourceConfig.getId());
                tableUriToSrcNameMap.put(sourceConfig.getUri().toLowerCase(), sourceConfig.getName());
            }

            mysqlTxnManager =
                    new MysqlTransactionManagerImpl<T>(logid,tableUriToSrcIdMap, tableUriToSrcNameMap, schemaRegistryService,
                            this,sourceEventConsumer);
            mysqlTxnManager.setShutdownRequested(false);
            OpenReplicationListener orl =
                    new OpenReplicationListener(mysqlTxnManager, eventsMap,
                            binlogFilePrefix);
            openReplicator.setBinlogFileName(binlogFile);
            openReplicator.setBinlogPosition(offset);
            openReplicator.setBinlogEventListener(orl);
            openReplicator.start();
        }
        catch (URISyntaxException u)
        {
            LOGGER.error("Exception occurred while processing uri : " + u);
            return;
        }
        catch (InvalidConfigException e)
        {
            LOGGER.error("Exception occurred while processing uri : " + e);
            return;
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred while starting open replication.." + e);
            return;
        }
        LOGGER.info("Open Replicator has been started successfully for the file " + binlogFile);
    }

    /**
     * Extracts individual attributes such as username, password, hostname, port ,server id etc from the uri of the
     * format mysql://or_test%2For_test@localhost:3306/3306/mysql-bin
     * @param uri uri of the format mysql://or_test%2For_test@localhost:3306/3306/mysql-bin
     * @return returns bin log prefix
     */
    protected String processUri(URI uri) throws InvalidConfigException
    {
        String userInfo = uri.getUserInfo();
        if (null == userInfo)
        {
            String errorMessage = "missing user info in: " + uri;
            LOGGER.error(errorMessage);
            throw new InvalidConfigException(errorMessage);
        }
        int slashPos = userInfo.indexOf('/');
        if (slashPos < 0)
        {
            slashPos = userInfo.length();
        }
        else if (0 == slashPos)
        {
            String errorMessage = "missing user name in user info: " + userInfo;
            LOGGER.error(errorMessage);
            throw new InvalidConfigException(errorMessage);
        }
        String userName = userInfo.substring(0, slashPos);
        String userPass = slashPos < userInfo.length() - 1 ? userInfo.substring(slashPos + 1) : null;
        String hostName = uri.getHost();

        int port = uri.getPort();
        if (port < 0)
            port = DEFAULT_MYSQL_PORT;

        String path = uri.getPath();
        if (null == path)
        {
            String errorMessage = "missing path: " + uri;
            LOGGER.error(errorMessage);
            throw new InvalidConfigException(errorMessage);
        }
        Matcher matcher = PATH_PATTERN.matcher(path);
        if (!matcher.matches())
        {
            String errorMessage = "invalid path:" + path;
            LOGGER.error(errorMessage);
            throw new InvalidConfigException(errorMessage);
        }
        String[] group = matcher.group().split("/");
        if (group.length != 3)
        {
            String errorMessage = "Invalid format " + Arrays.toString(group);
            LOGGER.error(errorMessage);
            throw new InvalidConfigException(errorMessage);
        }
        String serverIdStr = group[SERVER_ID];

        int serverId = -1;
        try
        {
            serverId = Integer.parseInt(serverIdStr);
        }
        catch (NumberFormatException e)
        {
            String errorMessage = "incorrect mysql serverid:" + serverId;
            LOGGER.error(errorMessage);
            throw new InvalidConfigException(errorMessage);
        }

        /** Assign them to incoming variables */
        if (null != openReplicator)
        {
            openReplicator.setUser(userName);
            if (null != userPass)
            {
                openReplicator.setPassword(userPass);
            }
            openReplicator.setHost(hostName);
            openReplicator.setPort(port);
            openReplicator.setServerId(serverId);
        }
        LOGGER.debug("Extracted bin log prefix is " + group[BIN_LOG_PREFIX]);
        return group[BIN_LOG_PREFIX];
    }

    /**
     * Returns the logid ( upper 32 bits of the SCN )
     * For e.g., mysql-bin.000001 is said to have an id 000001
     * @param scn system change number
     * @return logid
     */
    public static int logid(long scn)
    {
        if (scn == -1 || scn == 0)
        {
            return 1;
        }
        return (int) ((scn >> 32) & 0xFFFFFFFF);
    }

    /**
     * Returns the binlogoffset ( lower 32 bits of the SCN )
     * @param scn system change number
     * @return binlogoffset
     */
    public static int offset(long scn)
    {
        if (scn == -1 || scn == 0)
        {
            return 4;
        }
        return (int) (scn & 0xFFFFFFFF);
    }


    /** Getters and Setters for this class */
    public void setEventsMap(Map<Integer, BinLogEventProcessor> eventsMap){
        this.eventsMap = eventsMap;
    }

    @Override
    public String getName()
    {
        return this.getClass().getName();
    }

    @Override
    public long getSCN()
    {
        return this.sinceSCN.get();
    }

    /**
     * Setting Updated SCN into Metrics and producer
     * @param latestScn
     */
    public void updateSCN(long latestScn)
    {
        this.sinceSCN.set(latestScn);
        this.metricsCollector.setProducerSCN(this.name,latestScn);
    }


    @Override
    public boolean isRunning()
    {
        return this.openReplicator.isRunning();
    }

    @Override
    public boolean isPaused()
    {
        return !this.openReplicator.isRunning();
    }

    @Override
    public void unpause()
    {
        throw new UnsupportedOperationException("'unpause' is not supported on this event producer");
    }

    @Override
    public void pause()
    {
        throw new UnsupportedOperationException("'unpause' is not supported on this event producer");
    }

    @Override
    public void shutdown()
    {
        try
        {
            LOGGER.info("Shutdown has been requested. MYSQLEventProducer shutting down");
            this.openReplicator.stop(5, TimeUnit.SECONDS);
            LOGGER.info("### Bootstrap Process completed successfully ###");
            LOGGER.info("Time Taken:" + (System.nanoTime() - startTime));
        }
        catch (Exception e)
        {
            LOGGER.error("Error while stopping mysql bootstrap", e);
        }
    }

    @Override
    public void waitForShutdown() throws InterruptedException, IllegalStateException
    {
        throw new UnsupportedOperationException("'waitForShutdown' is not supported on this event producer");
    }

    @Override
    public void waitForShutdown(long l) throws InterruptedException, IllegalStateException
    {
        throw new UnsupportedOperationException("'waitForShutdown' is not supported on this event producer");
    }
}
