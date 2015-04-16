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
package com.flipkart.aesop.runtime.producer.txnprocessor.impl;

import com.flipkart.aesop.runtime.producer.MysqlEventProducer;
import com.flipkart.aesop.runtime.producer.avro.MysqlAvroEventManager;
import com.flipkart.aesop.runtime.producer.mapper.BinLogEventMapper;
import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;
import com.flipkart.aesop.runtime.producer.txnprocessor.MysqlTransactionManager;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.PerSourceTransaction;
import com.linkedin.databus2.producers.ds.Transaction;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import org.apache.avro.generic.GenericRecord;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <code>MysqlTransactionManagerImpl</code> provides Mysql transaction lifecycle management. Provides functionalities to
 * begin, end and reset transactions and begin and end sources.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
@SuppressWarnings ("rawtypes")
public class MysqlTransactionManagerImpl<T extends GenericRecord> implements MysqlTransactionManager
{
    /** Logger for this class */
    private static final Logger LOGGER = LogFactory.getLogger(MysqlTransactionManagerImpl.class);
    /** Databus Event Buffer to which all the events are appended */
    private final DbusEventBufferAppendable eventBuffer;
    /** Max SCN Reader Writter to persist max SCN */
    private final MaxSCNReaderWriter maxSCNReaderWriter;
    /** Databus events statistics collector */
    private final DbusEventsStatisticsCollector dbusEventsStatisticsCollector;
    /** Event Manager map containing mapping of source to avro event Manager */
    private final Map<Integer, MysqlAvroEventManager<T>> eventManagerMap;
    /** SCN tracker */
    private final AtomicLong sinceSCN;
    /** table name to source id mapping */
    private final Map<String, Short> tableUriToSrcIdMap;
    /** table name to source name mapping */
    private final Map<String, String> tableUriToSrcNameMap;
    /** Current table name of events being handled */
    private String currTableName = "";
    /** Current active transaction */
    private Transaction transaction = null;
    /** Current table id of events being handled */
    private long currTableId = -1;
    /** Persource transaction */
    private PerSourceTransaction perSourceTransaction = null;
    /** Tracks begin of a transaction */
    private boolean beginTxnSeen = false;
    /** Size of current transaction */
    private long currTxnSizeInBytes = 0;
    /** Current Transaction timestamp */
    private long currTxnTimestamp = 0;
    /** Current Transaction timestamp */
    private long currTxnStartReadTimestamp = 0;
    /** Current bin log file number */
    private int currFileNum;
    /** Bin log event mappers for mapping individual bin log events */
    private Map<Integer, BinLogEventMapper<T>> binLogEventMappers;
    /** Schema registry service which maintains currently active schemas */
    private SchemaRegistryService schemaRegistryService;
    /** mysqlTableId to tableName mapping */
    private Map<Long, String> mysqlTableIdToTableNameMap;
    private MysqlEventProducer mySqlEventProducer;
    /** The SCN generator implementation to use*/
    private SCNGenerator scnGenerator;

    private volatile AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    /** Constructor for the class */
    public MysqlTransactionManagerImpl(final DbusEventBufferAppendable eventBuffer,
                                       final MaxSCNReaderWriter maxSCNReaderWriter,
                                       final DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
                                       final Map<Integer, MysqlAvroEventManager<T>> eventManagerMap, final int currFileNum,
                                       final Map<String, Short> tableUriToSrcIdMap, final Map<String, String> tableUriToSrcNameMap,
                                       final SchemaRegistryService schemaRegistryService, final AtomicLong sinceSCN,
                                       Map<Integer, BinLogEventMapper<T>> binLogEventMappers, SCNGenerator scnGenerator,
                                       MysqlEventProducer mySqlEventProducer){
        this.eventBuffer = eventBuffer;
        this.maxSCNReaderWriter = maxSCNReaderWriter;
        this.dbusEventsStatisticsCollector = dbusEventsStatisticsCollector;
        this.eventManagerMap = eventManagerMap;
        this.currFileNum = currFileNum;
        this.tableUriToSrcIdMap = tableUriToSrcIdMap;
        this.tableUriToSrcNameMap = tableUriToSrcNameMap;
        this.schemaRegistryService = schemaRegistryService;
        this.sinceSCN = sinceSCN;
        this.binLogEventMappers = binLogEventMappers;
        this.mysqlTableIdToTableNameMap = new HashMap<Long, String>();
        this.scnGenerator = scnGenerator;
        this.mySqlEventProducer = mySqlEventProducer;
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
     * @see com.flipkart.aesop.runtime.producer.txnprocessor.TransactionProcessor#endXtion(long)
     */
    @Override
    public void endXtion(long eventTimeStamp)
    {
        if(!shutdownRequested.get())
        {
            currTxnTimestamp = eventTimeStamp * 1000000L;
            long txnReadLatency = System.nanoTime() - currTxnStartReadTimestamp;
            try
            {
                if (transaction.getScn() != -1)
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
     * Identifies if source has changed and if changed starts new source.
     * @param newTableId
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
                List<DbChangeEntry> entries =
                        eventManagerMap.get(Integer.valueOf(tableUriToSrcIdMap.get(currTableName))).frameAvroRecord(
                                eventHeader, rowList, databusOpcode, binLogEventMappers, schema.getSchema(),
                                this.scnGenerator.getSCN(frameSCN(currFileNum, (int) eventHeader.getPosition()),
                                        this.mySqlEventProducer.getBinLogHost()));
                for (DbChangeEntry entry : entries)
                {
                    perSourceTransaction.mergeDbChangeEntrySet(entry);
                }
                /**
                 * This is added here, because, if the mergePersource TXN is called during setSource,
                 * then , in the scenario where-in MysqlTransactionManagerImpl#perSourceTxn needs to be recycled,
                 * the DBEntry obtained above are not yet added to the new Instance of perSourceTransaction
                 */
                if(!perSourceTransaction.equals(transaction.getPerSourceTransaction(perSourceTransaction.getSrcId())))  {
                    /**
                     * this If check signifies that the current per source txn is a source that we have already processed
                     * but we are receiving fresh events for it, so we need to merge it with the old one.
                     */
                    transaction.mergePerSourceTransaction(perSourceTransaction);
                }

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

    /** Getters/Setters for the members */
    @Override
    public String getCurrTableName()
    {
        return currTableName;
    }

    @Override
    public void setCurrFileNum(int currFileNum)
    {
        this.currFileNum = currFileNum;
    }

    @Override
    public long getCurrTableId()
    {
        return currTableId;
    }

    public void setCurrTableName(String currTableName)
    {
        this.currTableName = currTableName;
    }

    public Transaction getTransaction()
    {
        return transaction;
    }

    public void setTransaction(Transaction transaction)
    {
        this.transaction = transaction;
    }

    public void setCurrTableId(long currTableId)
    {
        this.currTableId = currTableId;
    }

    public PerSourceTransaction getPerSourceTransaction()
    {
        return perSourceTransaction;
    }

    public void setPerSourceTransaction(PerSourceTransaction perSourceTransaction)
    {
        this.perSourceTransaction = perSourceTransaction;
    }

    public boolean isBeginTxnSeen()
    {
        return beginTxnSeen;
    }

    public void setBeginTxnSeen(boolean beginTxnSeen)
    {
        this.beginTxnSeen = beginTxnSeen;
    }

    public long getCurrTxnSizeInBytes()
    {
        return currTxnSizeInBytes;
    }

    public void setCurrTxnSizeInBytes(long currTxnSizeInBytes)
    {
        this.currTxnSizeInBytes = currTxnSizeInBytes;
    }

    public long getCurrTxnTimestamp()
    {
        return currTxnTimestamp;
    }

    public void setCurrTxnTimestamp(long currTxnTimestamp)
    {
        this.currTxnTimestamp = currTxnTimestamp;
    }

    public long getCurrTxnStartReadTimestamp()
    {
        return currTxnStartReadTimestamp;
    }

    public void setCurrTxnStartReadTimestamp(long currTxnStartReadTimestamp)
    {
        this.currTxnStartReadTimestamp = currTxnStartReadTimestamp;
    }

    public int getCurrFileNum()
    {
        return currFileNum;
    }

    public Map<Integer, BinLogEventMapper<T>> getBinLogEventMappers()
    {
        return binLogEventMappers;
    }

    public void setBinLogEventMappers(Map<Integer, BinLogEventMapper<T>> binLogEventMapper)
    {
        this.binLogEventMappers = binLogEventMapper;
    }

    public SchemaRegistryService getSchemaRegistryService()
    {
        return schemaRegistryService;
    }

    public void setSchemaRegistryService(SchemaRegistryService schemaRegistryService)
    {
        this.schemaRegistryService = schemaRegistryService;
    }

    public DbusEventBufferAppendable getEventBuffer()
    {
        return eventBuffer;
    }

    public MaxSCNReaderWriter getMaxSCNReaderWriter()
    {
        return maxSCNReaderWriter;
    }

    public DbusEventsStatisticsCollector getDbusEventsStatisticsCollector()
    {
        return dbusEventsStatisticsCollector;
    }

    public Map<Integer, MysqlAvroEventManager<T>> getEventFactoryMap()
    {
        return eventManagerMap;
    }

    public AtomicLong getSinceSCN()
    {
        return sinceSCN;
    }

    public Map<String, Short> getTableUriToSrcIdMap()
    {
        return tableUriToSrcIdMap;
    }

    public Map<String, String> getTableUriToSrcNameMap()
    {
        return tableUriToSrcNameMap;
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

    /**
     * Invoked by {@link MysqlTransactionManagerImpl#endXtion(long)}.Adds events to event buffer and saves the maximum
     * SCN.
     * @param txn transaction to end
     */
    private void onEndTransaction(Transaction txn) throws DatabusException
    {
        try
        {
            addTxnToBuffer(txn);
            maxSCNReaderWriter.saveMaxScn(txn.getScn());
            mySqlEventProducer.updateSCN(txn.getScn());
        }
        catch (UnsupportedKeyException e)
        {
            LOGGER.error("Got UnsupportedKeyException exception while adding txn (" + txn + ") to the buffer", e);
            throw new DatabusException(e);
        }
        catch (EventCreationException e)
        {
            LOGGER.error("Got EventCreationException exception while adding txn (" + txn + ") to the buffer", e);
            throw new DatabusException(e);
        }
    }

    /**
     * Invoked by {@link MysqlTransactionManagerImpl#onEndTransaction(Transaction)}.Adds events to event buffer.
     * @param txn transaction to be persisted
     */
    private void addTxnToBuffer(Transaction txn) throws DatabusException, UnsupportedKeyException,
            EventCreationException
    {
        eventBuffer.startEvents();
        long scn = txn.getScn();
        for (PerSourceTransaction t : txn.getOrderedPerSourceTransactions())
        {
            for (DbChangeEntry changeEntry : t.getDbChangeEntrySet())
            {
                int length = 0;
                try
                {
                    length =
                            eventManagerMap.get(t.getSrcId()).createAndAppendEvent(changeEntry, eventBuffer, false,
                                    dbusEventsStatisticsCollector);
                    if (length < 0)
                    {
                        LOGGER.error("Unable to append DBChangeEntry (" + changeEntry
                                + ") to event buffer !! EVB State : " + eventBuffer);
                        throw new DatabusException("Unable to append DBChangeEntry (" + changeEntry
                                + ") to event buffer !!");
                    }
                    LOGGER.debug("Added entry " + changeEntry + " successfully to event buffer");
                }
                catch (DatabusException e)
                {
                    LOGGER.error("Got databus exception :", e);
                    throw e;
                }
                catch (UnsupportedKeyException e)
                {
                    LOGGER.error("Got UnsupportedKeyException :", e);
                    throw e;
                }
                catch (EventCreationException e)
                {
                    LOGGER.error("Got EventCreationException :", e);
                    throw e;
                }
            }
        }
        eventBuffer.endEvents(scn, dbusEventsStatisticsCollector);
    }

    @Override
    public void setShutdownRequested(boolean shutdownRequested)
    {
        this.shutdownRequested.set(shutdownRequested);
    }

}
