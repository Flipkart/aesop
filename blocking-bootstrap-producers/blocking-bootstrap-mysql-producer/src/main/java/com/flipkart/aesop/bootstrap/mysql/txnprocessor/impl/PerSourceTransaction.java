package com.flipkart.aesop.bootstrap.mysql.txnprocessor.impl;

import com.flipkart.aesop.bootstrap.mysql.MysqlEvent;

import java.util.HashSet;
import java.util.Set;

public class PerSourceTransaction
{
    /**
     * Databus Source Id of the Source
     */
    private final int _srcId;

    /**
     * Mysql Event set for this source in the current txn. The changeEntry is keyed by the pKeys.
     * So, A given key appears only once in the PerSourceTransactionObject
     */
    private Set<MysqlEvent> sourceEventChangeSet;

    public PerSourceTransaction(int srcId, Set<MysqlEvent> sourceEventChangeSet)
    {
        this._srcId = srcId;
        this.sourceEventChangeSet = sourceEventChangeSet;

        if ( null == sourceEventChangeSet)
            this.sourceEventChangeSet = new HashSet<MysqlEvent>();
    }

    public PerSourceTransaction(int srcId) {
        this(srcId, null);
    }

    public Set<MysqlEvent> getSourceEventChangeSet() {
        return sourceEventChangeSet;
    }

    /**
     * Add a DB changeEntry to the PerSourceTransaction.
     * If an entry exist for the same key(s), it will be overwritten with the passed value
     *
     * @param mysqlEvent Abstract Event to be added
     */
    public void mergeDbChangeEntrySet(MysqlEvent mysqlEvent)
    {
        if ( sourceEventChangeSet.contains(mysqlEvent))
        {
            sourceEventChangeSet.remove(mysqlEvent);
        }

        sourceEventChangeSet.add(mysqlEvent);
    }

    /**
     * Get source id of the source for which this perSourceTxn object corresponds to
     * @return sourceId
     */
    public int getSrcId() {
        return _srcId;
    }

    /**
     *
     * Get the max Scn among all dbChangeEntry set
     * @return
     */
    public long getScn()
    {
        long maxScn = -1;

        for (MysqlEvent c : sourceEventChangeSet)
        {
            maxScn = Math.max(maxScn,c.getScn());
        }
        return maxScn;
    }
}
