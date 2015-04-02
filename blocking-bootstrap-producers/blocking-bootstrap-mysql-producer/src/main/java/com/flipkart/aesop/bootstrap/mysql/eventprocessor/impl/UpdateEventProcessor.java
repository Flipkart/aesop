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

package com.flipkart.aesop.bootstrap.mysql.eventprocessor.impl;

import java.util.ArrayList;
import java.util.List;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.AbstractBinLogEventProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;

/**
 * The <code>UpdateEventProcessor</code> processes UpdateRowsEvent from source. This event is received if there is any
 * update operation on the source.
 * @author nrbafna
 */
public class UpdateEventProcessor<T extends AbstractEvent> extends AbstractBinLogEventProcessor<T>
{
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener<T> listener)
	{
		UpdateRowsEvent updateRowsEvent = (UpdateRowsEvent) event;

		List<Pair<Row>> listOfPairs = updateRowsEvent.getRows();
		List<Row> rowList = new ArrayList<Row>(listOfPairs.size());
		for (Pair<Row> pair : listOfPairs)
		{
			Row row = pair.getAfter();
			rowList.add(row);
		}

		List<AbstractEvent> sourceEvents = map(updateRowsEvent.getTableId(), rowList, listener, DbusOpcode.UPSERT);
		for (AbstractEvent sourceEvent : sourceEvents)
		{
			listener.getSourceEventConsumer().onEvent(sourceEvent);
		}
	}
}
