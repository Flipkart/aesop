package com.flipkart.aesop.bootstrap.mysql.txnprocessor.impl;


import com.flipkart.aesop.bootstrap.mysql.MysqlEvent;

import java.util.*;

public class Transaction
{
    /**
     * Collections of PerSourceTransactions keyed by databus source id.
     */
    private Map<Integer, PerSourceTransaction> _perSourceTxnEntries;

    /**
     * Size of Transaction in the source
     */
    private long _sizeInBytes;

    /**
     * Transaction Read time latency
     */
    private long _txnReadLatencyNanos;

    /**
     * Transaction's timestamp at source
     */
    private long _txnNanoTimestamp;

    public Transaction()
    {
        _perSourceTxnEntries = new HashMap<Integer,PerSourceTransaction>();
    }

    /**
     * Used for incrementally building transaction object. Old DbCHange objects corresponding
     * to a primary key will be overwritten by the new dbCHange entries.
     *
     * @param newPerSourceTxn New Transaction instance to merge.
     */
    public void mergePerSourceTransaction(PerSourceTransaction newPerSourceTxn)
    {
        int srcId = newPerSourceTxn.getSrcId();
        if (_perSourceTxnEntries.containsKey(srcId))
        {
            PerSourceTransaction oldPerSourceTxn = _perSourceTxnEntries.get(srcId);

            for (MysqlEvent mysqlEvent : newPerSourceTxn.getSourceEventChangeSet())
            {
                oldPerSourceTxn.mergeDbChangeEntrySet(mysqlEvent);
            }
        }
        else
        {
            // No such entry, just add the PerSourceTransaction for the source to map
            _perSourceTxnEntries.put(srcId, newPerSourceTxn);
        }

    }

    /**
     * Get the Per Source Transaction object corresponding to the source-id.
     *
     * @param srcId Source Id
     * @return
     */
    public PerSourceTransaction getPerSourceTransaction(int srcId)
    {
        return _perSourceTxnEntries.get(srcId);
    }

    /**
     * Get byte size of the transaction as seen in the source log/DB
     *
     * @return
     */
    public long getSizeInBytes()
    {
        return _sizeInBytes;
    }

    public void setSizeInBytes(long _sizeInBytes)
    {
        this._sizeInBytes = _sizeInBytes;
    }

    /**
     * Get Latency for capturing this transaction
     * @return
     */
    public long getTxnReadLatencyNanos()
    {
        return _txnReadLatencyNanos;
    }

    public void setTxnReadLatencyNanos(long _txnReadLatencyNanos)
    {
        this._txnReadLatencyNanos = _txnReadLatencyNanos;
    }

    /**
     * Get Timestamp (nano) of this transaction
     * @return
     */
    public long getTxnNanoTimestamp()
    {
        return _txnNanoTimestamp;
    }

    public void setTxnNanoTimestamp(long _txnNanoTimestamp)
    {
        this._txnNanoTimestamp = _txnNanoTimestamp;
    }

    /**
     * Returns the list of PerSourceTransactions in the order of the SourceIds.
     * @return PerSourceTransaction list
     */
    public List<PerSourceTransaction> getOrderedPerSourceTransactions()
    {
        List<PerSourceTransaction> txns = new ArrayList<PerSourceTransaction>(_perSourceTxnEntries.values());
        Collections.sort(txns, new Comparator<PerSourceTransaction>()
        {

            @Override
            public int compare(PerSourceTransaction o1, PerSourceTransaction o2)
            {
                return new Integer(o1.getSrcId()).compareTo(o2.getSrcId());
            }
        });
        return txns;
    }

    /**
     * Get Max Scn among all dbChange Entries in this transaction object
     * @return
     */
    public long getScn()
    {
        long maxScn = -1;

        for (PerSourceTransaction t : _perSourceTxnEntries.values())
        {
            maxScn = Math.max(maxScn,t.getScn());
        }
        return maxScn;
    }
}
