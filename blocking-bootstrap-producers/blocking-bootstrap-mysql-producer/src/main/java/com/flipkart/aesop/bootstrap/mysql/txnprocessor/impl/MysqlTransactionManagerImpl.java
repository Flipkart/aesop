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
package com.flipkart.aesop.bootstrap.mysql.txnprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.MysqlEvent;
import com.flipkart.aesop.bootstrap.mysql.MysqlEventProducer;
import com.flipkart.aesop.bootstrap.mysql.mapper.BinLogEventMapper;
import com.flipkart.aesop.bootstrap.mysql.mapper.impl.DefaultBinLogEventMapper;
import com.flipkart.aesop.bootstrap.mysql.txnprocessor.MysqlTransactionManager;
import com.flipkart.aesop.bootstrap.mysql.utils.ORToMysqlMapper;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.runtime.bootstrap.consumer.SourceEventConsumer;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <code>MysqlTransactionManagerImpl</code> provides Mysql transaction lifecycle management. Provides functionalities to
 * begin, end and reset transactions and begin and end sources.
 */
@SuppressWarnings ("rawtypes")
public class MysqlTransactionManagerImpl<T extends AbstractEvent> implements MysqlTransactionManager
{
    /* ========================= Variables =============================*/
    /** Logger for this class */
    private static final Logger LOGGER = LogFactory.getLogger(MysqlTransactionManagerImpl.class);

    private volatile AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    /* ========================= Mysql File/Table Mapping Variables =============================*/

    /** Current bin log file number */
    private int currFileNum;
    /** Current table name of events being handled */
    private String currTableName = "";
    /** Current table id of events being handled */
    private long currTableId = -1;
    /** Tracks begin of a transaction */
    private boolean beginTxnSeen = false;
    /** Size of current transaction */
    private long currTxnSizeInBytes = 0;
    /** Current Transaction timestamp */
    private long currTxnStartReadTimestamp = 0;

    /** mysqlTableId to tableName mapping */
    private Map<Long, String> mysqlTableIdToTableNameMap;
    /** table name to source id mapping */
    private final Map<String, Short> tableUriToSrcIdMap;
    /** table name to source name mapping */
    private final Map<String, String> tableUriToSrcNameMap;

    /* ========================= Aesop Producer =============================*/

    /** Bin log event mappers for mapping individual bin log events */
    private final BinLogEventMapper<T> binLogEventMapper;
    /** Current active transaction */
    private Transaction transaction = null;
    /** Persource transaction */
    private PerSourceTransaction perSourceTransaction = null;
    /** Schema registry service which maintains currently active schemas */
    private SchemaRegistryService schemaRegistryService;
    /* Mysql Event Producer */
    private MysqlEventProducer mySqlEventProducer;

    /* ========================= Aesop Consumer =============================*/
    private final SourceEventConsumer sourceEventConsumer;


    public MysqlTransactionManagerImpl(final int currFileNum,
                                       final Map<String, Short> tableUriToSrcIdMap, final Map<String, String> tableUriToSrcNameMap,
                                       final SchemaRegistryService schemaRegistryService,
                                       final MysqlEventProducer mysqlEventProducer,
                                       SourceEventConsumer sourceEventConsumer){
        this.currFileNum = currFileNum;
        this.tableUriToSrcIdMap = tableUriToSrcIdMap;
        this.tableUriToSrcNameMap = tableUriToSrcNameMap;
        this.schemaRegistryService = schemaRegistryService;
        this.mysqlTableIdToTableNameMap = new HashMap<Long, String>();
        this.sourceEventConsumer = sourceEventConsumer;
        this.binLogEventMapper = new DefaultBinLogEventMapper<T>(new ORToMysqlMapper());
        this.mySqlEventProducer = mysqlEventProducer;
    }

    /**
     * Creates new transaction
     */
    @Override
    public void startXtion()
    {
        currTxnStartReadTimestamp = System.nanoTime();
        if (transaction == null)
        {
            transaction = new Transaction();
        }
        else
        {
            LOGGER.warn("Illegal Start Transaction State");
            throw new DatabusRuntimeException("Got startXtion without an endXtion for previous transaction");
        }
    }

    /**
     * Persists all the transaction data and ends the current transaction.
     * @param eventTimeStamp time when transaction end event was generated
     */
    @Override
    public void endXtion(long eventTimeStamp)
    {
        if(!shutdownRequested.get())
        {
            long currTxnTimestamp = eventTimeStamp * 1000000L;
            long txnReadLatency = System.nanoTime() - currTxnStartReadTimestamp;
            try
            {
                transaction.setSizeInBytes(currTxnSizeInBytes);
                transaction.setTxnNanoTimestamp(currTxnTimestamp);
                transaction.setTxnReadLatencyNanos(txnReadLatency);
                try
                {
                    onEndTransaction(transaction);
                }
                catch (DatabusException e3)
                {
                    LOGGER.error("Got exception in the transaction handler ", e3);
                    throw new DatabusRuntimeException(e3);
                }
            }
            finally
            {
                resetTxn();
            }
        }
        else
        {
            LOGGER.info("Not writing event to buffer as shutdown has been requested");
        }
    }

    /**
     * Resets the transaction in case of rollback. Resets the transaction state.
     */
    @Override
    public void resetTxn()
    {
        currTableName = "";
        currTableId = -1;
        perSourceTransaction = null;
        transaction = null;
        currTxnSizeInBytes = 0;
    }

    /**
     * Identifies if source has changed and if changed starts new source.
     * @param newTableId the table Id for the current record
     */
    @Override
    public void setSource(long newTableId)
    {
        String newTableName = mysqlTableIdToTableNameMap.get(newTableId);
        if (null == newTableName)
        {
            LOGGER.error("TableMap Event not received for the change event tableId: " + newTableId);
            throw new DatabusRuntimeException("TableMap Event not received for the change event tableId: " + newTableId);
        }
        else if (this.currTableName.isEmpty() && (this.currTableId == -1))
        {
            /** First changeEvent for the transaction. */
            startSource(newTableName, newTableId);
        }
        else if (!this.currTableName.equals(newTableName) || this.currTableId != newTableId)
        {
            LOGGER.debug("Table name changed from " + this.currTableName + " to " + newTableName);
            endSource();
            startSource(newTableName, newTableId);
        }
        else
        {
            /** change event from the current source */
        }
    }

    @Override
    public Map<Long, String> getMysqlTableIdToTableNameMap()
    {
        return this.mysqlTableIdToTableNameMap;
    }

    /**
     * Persists event related data in transaction object
     * @param eventHeader Binary log event header
     * @param rowList list of mutated rows
     * @param databusOpcode operation code indicating nature of change such as insertion,deletion or updation.
     */
    @Override
    public void performChanges(long tableId, BinlogEventV4Header eventHeader, List<Row> rowList,
                               final DbusOpcode databusOpcode)
    {
        try
        {
            setSource(tableId);
            VersionedSchema schema =
                    schemaRegistryService.fetchLatestVersionedSchemaBySourceName(tableUriToSrcNameMap
                            .get(currTableName));
            LOGGER.debug("Schema obtained for table " + currTableName + " = " + schema);
            if (schema != null)
            {
                for (Row row : rowList)
                {
                    AbstractEvent abstractEvent =binLogEventMapper.mapBinLogEvent(row, schema.getSchema(),
                            databusOpcode);
                    perSourceTransaction.mergeDbChangeEntrySet(new MysqlEvent(frameSCN(currFileNum, (int) eventHeader.getPosition()),abstractEvent));
                }


                /**
                 * This is added here, because, if the mergePersource TXN is called during setSource,
                 * then , in the scenario where-in MysqlTransactionManagerImpl#perSourceTxn needs to be recycled,
                 * the DBEntry obtained above are not yet added to the new Instance of perSourceTransaction
                 */
                if(!perSourceTransaction.equals(transaction.getPerSourceTransaction(perSourceTransaction.getSrcId())))
                    transaction.mergePerSourceTransaction(perSourceTransaction);
            }
            else
            {
                LOGGER.info("Events recieved from uninterested sources " + currTableName);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            LOGGER.error("Exception occurred while persisting changes to transaction " + e.getMessage());
        }
    }

    @Override
    public void setShutdownRequested(boolean shutdownRequested)
    {
        this.shutdownRequested.set(shutdownRequested);
    }

    /**
     * starts new source
     * @param newTableName table name from which event is being received
     * @param newTableId table id from which event is being received
     */

    private void startSource(String newTableName, long newTableId)
    {
        currTableName = newTableName;
        currTableId = newTableId;
        if (perSourceTransaction == null || transaction == null)
        {
            Short srcId = tableUriToSrcIdMap.get(currTableName);
            if (null == srcId)
            {
                /** Only case when perSourceTransaction will be null in the end source call */
                LOGGER.warn("Could not find a matching logical source for table Uri (" + currTableName + ")");
                return;
            }
            perSourceTransaction = new PerSourceTransaction(srcId);
            transaction.mergePerSourceTransaction(perSourceTransaction);
        }
        else
        {
            String errorMessage = "Seems like a startSource has been received without an endSource for previous source";
            LOGGER.error(errorMessage);
            throw new DatabusRuntimeException(errorMessage);
        }
    }

    /**
     * ends the current source
     */
    private void endSource()
    {
        perSourceTransaction = null;
    }

    /**
     * Invoked by {@link MysqlTransactionManagerImpl#endXtion(long)}.Adds events to event buffer and saves the maximum
     * SCN.
     * @param txn transaction to end
     * @throws DatabusException dataBus Exception
     */
    private void onEndTransaction(Transaction txn) throws DatabusException
    {
        sendEventsToSourceEventConsumer(txn);
        mySqlEventProducer.updateSCN(txn.getScn());
    }

    /**
     *  Sends the Source Events to the Source Event Consumer
     * @param txn txn Object
     */
    private void sendEventsToSourceEventConsumer(Transaction txn)
    {
        for (PerSourceTransaction t : txn.getOrderedPerSourceTransactions())
        {
            for (MysqlEvent mysqlEvent : t.getSourceEventChangeSet())
            {
                sourceEventConsumer.onEvent(mysqlEvent.getAbstractEvent());
            }
        }
    }

    /**
     * Frames SCN from logid and offset. Lower 32 bits are offset and higher 32 bits are logid
     * @param logId bin log file number
     * @param offset position in bin log file to start retrieval from
     * @return scn system change number
     */
    private long frameSCN(int logId, int offset)
    {
        long scn = logId;
        scn <<= 32;
        scn |= offset;
        return scn;
    }

    /** Getters/Setters for the members */
    @Override
    public String getCurrTableName() {
        return currTableName;
    }

    @Override
    public void setCurrFileNum(int currFileNum) {
        this.currFileNum = currFileNum;
    }

    @Override
    public long getCurrTableId() {
        return currTableId;
    }

    public boolean isBeginTxnSeen()  {
        return beginTxnSeen;
    }

    public void setBeginTxnSeen(boolean beginTxnSeen) {
        this.beginTxnSeen = beginTxnSeen;
    }
}
