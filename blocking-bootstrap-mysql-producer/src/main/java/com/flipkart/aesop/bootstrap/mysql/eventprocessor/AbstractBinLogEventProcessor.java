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

package com.flipkart.aesop.bootstrap.mysql.eventprocessor;

import java.util.ArrayList;
import java.util.List;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.eventlistener.OpenReplicationListener;
import com.flipkart.aesop.event.AbstractEvent;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.schemas.VersionedSchema;

/**
 * <code>AbstractBinLogEventProcessor</code> provides utility to map bin log events to list of
 * {@link com.flipkart.aesop.event.AbstractEvent}
 * @author nrbafna
 */
public abstract class AbstractBinLogEventProcessor implements BinLogEventProcessor
{
	public static final Logger LOGGER = LogFactory.getLogger(AbstractBinLogEventProcessor.class);

	protected List<AbstractEvent> map(long tableId, List<Row> rowList, OpenReplicationListener listener)
	{
		String tableName = listener.getTableIdtoNameMapping().get(tableId);
		return map(tableName, rowList, listener);
	}

	protected List<AbstractEvent> map(String tableName, List<Row> rowList,
	        OpenReplicationListener openReplicationListener)
	{
		List<AbstractEvent> sourceEvents = new ArrayList<AbstractEvent>();

		try
		{
			if (openReplicationListener.getInterestedSourceList().contains(tableName))
			{
				VersionedSchema schema =
				        openReplicationListener.getSchemaRegistryService().fetchLatestVersionedSchemaBySourceName(
				                openReplicationListener.getTableUriToSrcNameMap().get(tableName));

				for (Row row : rowList)
				{
					AbstractEvent abstractEvent =
					        openReplicationListener.getBinLogEventMapper().mapBinLogEvent(row, schema.getSchema(),
					                DbusOpcode.UPSERT);
					sourceEvents.add(abstractEvent);
				}
			}
			else
			{
				LOGGER.info("Event received form uninterested source");
			}
		}
		catch (Exception ex)
		{
			LOGGER.error("Exception : ", ex);
		}

		return sourceEvents;
	}
}
