package com.flipkart.aesop.runtime.producer.schema.eventprocessor.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.avro.schemagenerator.main.SchemaGenerator;
import com.flipkart.aesop.runtime.producer.schema.eventprocessor.SchemaChangeEventProcessor;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.VersionedSchemaId;

/**
 * <code>UpdateSchemaChangeEventProcessor</code> update the existing schemaRegistry service
 * @author yogesh.dahiya
 */

public class UpdateSchemaChangeEventProcessor implements SchemaChangeEventProcessor
{

	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(UpdateSchemaChangeEventProcessor.class);
	/** Instance of avro schema generator for schema creation */
	private SchemaGenerator schemaGenerator;
	/** Instance of schemaRegistryService to update */
	private SchemaRegistryService schemaRegistryService;
	/** schemaRegistry location */
	private String schemaRegistryLocation;
	/** table uri to source name mapping */
	private Map<String, String> tableUriToSrcNameMap;
	/** Regex to parse alter table query */
	private static final Pattern ALTER_TABLE_REGEX = Pattern.compile(
	        "(ALTER\\s+)(ONLINE\\s+|OFFLINE\\s+)?(IGNORE\\s+)?(TABLE\\s+)`(\\S+?)`.*", Pattern.CASE_INSENSITIVE);

	/**
	 * @param queryEvent for schema change event
	 *            (non-Javadoc)
	 * @see com.flipkart.aesop.runtime.producer.schema.eventprocessor.SchemaChangeEventProcessor#process(com.google.code.or.binlog.impl.event.QueryEvent)
	 */
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
		String tableUri = databaseName.toLowerCase() + "." + tableName.toLowerCase();
		VersionedSchema olderSchema =
		        schemaRegistryService.fetchLatestVersionedSchemaBySourceName(tableUriToSrcNameMap.get(tableUri));
		if (olderSchema != null)
		{
			this.process(databaseName, tableName);
		}
		else
		{
			LOGGER.info("Event recieved from uninterested source " + tableUri);
		}

	}

	/**
	 * update schemaRegistry for the given table
	 * @param databaseName
	 * @param tableName
	 * @throws Exception
	 */
	@Override
	public void process(String databaseName, String tableName) throws Exception
	{
		String newSchemaJson = schemaGenerator.generateSchema(databaseName, tableName);
		String tableUri = databaseName.toLowerCase() + "." + tableName.toLowerCase();
		VersionedSchema olderSchema =
		        schemaRegistryService.fetchLatestVersionedSchemaBySourceName(tableUriToSrcNameMap.get(tableUri));

		short olderVersion = (olderSchema != null) ? (short) olderSchema.getVersion() : 0;
		/** if the olderVersion is at its Max value then overwrite it */
		Short newVersion = olderVersion == Short.MAX_VALUE ? olderVersion : (short) (olderVersion + 1);
		VersionedSchema newSchema =
		        new VersionedSchema(new VersionedSchemaId(tableUriToSrcNameMap.get(tableUri), newVersion),
		                Schema.parse(newSchemaJson), null);
		schemaRegistryService.registerSchema(newSchema);
		if (SchemaRegistryService.class.isAssignableFrom(FileSystemSchemaRegistryService.class))
		{
			/** updating schemaRegistry source */
			String schemaFilePath =
			        schemaRegistryLocation + File.separator + tableUriToSrcNameMap.get(tableUri) + "."
			                + newVersion.toString() + ".avsc";
			File schemaFile = new File(schemaFilePath);
			if (schemaFile.exists())
			{
				schemaFile.delete();
				schemaFile = new File(schemaFilePath);
			}
			schemaFile.createNewFile();
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(schemaFile.getAbsoluteFile()));
			bufferedWriter.write(newSchemaJson);
			bufferedWriter.close();
		}

	}

	@Override
	public void setSchemaRegistryService(SchemaRegistryService schemaRegistryService)
	{
		this.schemaRegistryService = schemaRegistryService;
	}

	@Override
	public void setTableUriToSrcNameMap(Map<String, String> tableUriToSrcNameMap)
	{
		this.tableUriToSrcNameMap = tableUriToSrcNameMap;
	}

	public void setSchemaGenerator(SchemaGenerator schemaGenerator)
	{
		this.schemaGenerator = schemaGenerator;
	}

	public void setSchemaRegistryLocation(String schemaRegistryLocation)
	{
		this.schemaRegistryLocation = schemaRegistryLocation;
	}

}
