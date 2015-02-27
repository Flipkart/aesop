package com.flipkart.aesop.runtime.producer.schema.eventprocessor.impl;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.schema.eventprocessor.SchemaChangeEventProcessor;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * The <code>NopSchemaChangeEventProcessor</code> is nop implementation of SchemaChangeEventProcessor.
 * @author yogesh.dahiya
 */

public class NopSchemaChangeEventProcessor implements SchemaChangeEventProcessor
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(NopSchemaChangeEventProcessor.class);
	private static final Pattern ALTER_TABLE_REGEX = Pattern.compile(
	        "(ALTER\\s+)(ONLINE\\s+|OFFLINE\\s+)?(IGNORE\\s+)?(TABLE\\s+)`(\\S+?)`.*", Pattern.CASE_INSENSITIVE);

	@Override
	public void process(QueryEvent queryEvent) throws Exception
	{
		String sql = queryEvent.getSql().toString();
		Matcher matcher = ALTER_TABLE_REGEX.matcher(sql);
		if (!matcher.find() || matcher.group(5) == null)
		{
			throw new IllegalStateException("Failed to parse alter table sql");
		}
		String tableName = matcher.group(5);
		String databaseName = queryEvent.getDatabaseName().toString();
		LOGGER.info("QueryEvent : " + queryEvent.toString());
		LOGGER.info("Alter table received for table : " + databaseName + "." + tableName);
		LOGGER.info("schemaChangeEvent sql : " + sql);
		process(databaseName, tableName);
	}

	@Override
	public void process(String databaseName, String tableName) throws Exception
	{
		return;
	}

	@Override
	public void setSchemaRegistryService(SchemaRegistryService schemaRegistryService)
	{
		return;
	}

	@Override
	public void setTableUriToSrcNameMap(Map<String, String> tableUriToSrcNameMap)
	{
		return;
	}

}
