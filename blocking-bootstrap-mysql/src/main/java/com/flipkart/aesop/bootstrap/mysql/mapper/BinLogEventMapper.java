package com.flipkart.aesop.bootstrap.mysql.mapper;

import org.apache.avro.Schema;

import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;

/**
 * Created by nikhil.bafna on 1/22/15.
 */
public interface BinLogEventMapper<T extends AbstractEvent>
{
	public T mapBinLogEvent(Row row, Schema schema, DbusOpcode dbusOpCode);

	public String getUniqueName();
}
