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

import java.util.List;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.bootstrap.mysql.eventprocessor.AbstractBinLogEventProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.linkedin.databus.core.DbusOpcode;

/**
 * The <code>DeleteEventProcessor</code> processes DeleteRowsEvent from source. This event is received if there is any
 * delete operation on the source.
 * @author nrbafna
 */
public class DeleteEventProcessor extends AbstractBinLogEventProcessor
{
	@Override
	public void process(BinlogEventV4 event, OpenReplicationListener listener)
	{
		DeleteRowsEvent deleteEvent = (DeleteRowsEvent) event;
		List<AbstractEvent> sourceEvents =
		        map(deleteEvent.getTableId(), deleteEvent.getRows(), listener, DbusOpcode.DELETE);
		for (AbstractEvent sourceEvent : sourceEvents)
		{
			listener.getSourceEventConsumer().onEvent(sourceEvent);
		}
	}
}
