package com.flipkart.aesop.bootstrap.mysql.eventprocessor;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.google.code.or.binlog.BinlogEventV4;

/**
 * Created by nikhil.bafna on 1/27/15.
 */
public interface BinLogEventProcessor
{
	void process(BinlogEventV4 event, OpenReplicationListener listener);
}
