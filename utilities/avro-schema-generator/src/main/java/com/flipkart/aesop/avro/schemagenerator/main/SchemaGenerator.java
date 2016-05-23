package com.flipkart.aesop.avro.schemagenerator.main;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.InitializingBean;

import com.flipkart.aesop.avro.schemagenerator.data.TableRecord;
import com.flipkart.aesop.avro.schemagenerator.mysql.DataSourceConfig;
import com.flipkart.aesop.avro.schemagenerator.mysql.MysqlConnectionProvider;
import com.flipkart.aesop.avro.schemagenerator.mysql.MysqlUtils;

/**
 * <code>SchemaGenerator</code> generates avro schema.
 * To generate Schema dbConfig and schemaSettings must be injected.
 * @author chandan.bansal
 */
public class SchemaGenerator implements InitializingBean
{

	/** Configuration to initialize dataSources */
	private List<DataSourceConfig> dataSourceConfigs = new ArrayList<DataSourceConfig>();
	/** Map of database name to their table inclusionList */
	private Map<String, List<String>> tablesInclusionListMap = new HashMap<String, List<String>>();
	/** Map of database name to their table exclusionList */
	private Map<String, List<String>> tablesExclusionListMap = new HashMap<String, List<String>>();
	/** objectMapper to map json object to string. */
	private ObjectMapper objectMapper = new ObjectMapper();
	/** Json Factory to create json generator objects. */
	private JsonFactory jsonFactory = new JsonFactory();
	/** date formatter. */
	private SimpleDateFormat df = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a zzz");

	private String rowChangeFieldName = null;

	/**
	 * Instantiates a new schema generator.
	 * @param dataSourceConfigs - configuration to initialize datasource
	 * @param tablesInclusionListMap
	 * @param tablesExclusionListMap
	 * @throws PropertyVetoException the property veto exception
	 */
	public SchemaGenerator(List<DataSourceConfig> dataSourceConfigs, Map<String, List<String>> tablesInclusionListMap,
	        Map<String, List<String>> tablesExclusionListMap) throws PropertyVetoException
	{
		for (DataSourceConfig dataSourceConfig : dataSourceConfigs)
		{
			MysqlConnectionProvider.getInstance().addDataSource(dataSourceConfig);
		}
		this.tablesInclusionListMap = tablesInclusionListMap;
		this.tablesExclusionListMap = tablesExclusionListMap;

	}

	/**
	 * Instantiates a new schema generator.
	 * @param dataSourceConfigs - configuration to initialize datasource
	 * @param tablesInclusionListMap
	 * @param tablesExclusionListMap
	 * @param rowChangeFieldName
	 * @throws PropertyVetoException the property veto exception
	 */
	public SchemaGenerator(List<DataSourceConfig> dataSourceConfigs, Map<String, List<String>> tablesInclusionListMap,
	        Map<String, List<String>> tablesExclusionListMap, String rowChangeFieldName) throws PropertyVetoException
	{
		for (DataSourceConfig dataSourceConfig : dataSourceConfigs)
		{
			MysqlConnectionProvider.getInstance().addDataSource(dataSourceConfig);
		}
		this.tablesInclusionListMap = tablesInclusionListMap;
		this.tablesExclusionListMap = tablesExclusionListMap;
		this.rowChangeFieldName = rowChangeFieldName;
	}

	/**
	 * Constructor
	 * @param dataSourceConfigs
	 * @throws PropertyVetoException
	 */
	public SchemaGenerator(List<DataSourceConfig> dataSourceConfigs) throws PropertyVetoException
	{
		for (DataSourceConfig dataSourceConfig : dataSourceConfigs)
		{
			MysqlConnectionProvider.getInstance().addDataSource(dataSourceConfig);
		}
	}

	public SchemaGenerator()
	{

	}

	/**
	 * Generate schema for all tables of the given dataSourceId.
	 * @param dbName the db name
	 * @return table to schema mapping
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Map<String, String> generateSchemaForAllTables(String dbName) throws IOException, IllegalArgumentException
	{
		Map<String, String> tableNameToSchemaMap = new HashMap<String, String>();
		List<String> tableNameList = MysqlUtils.getTablesInDB(dbName);

		for (String tableName : tableNameList)
		{
			if ((tablesExclusionListMap.get(dbName) != null && tablesExclusionListMap.get(dbName).contains(tableName))
			        || (tablesInclusionListMap.get(dbName) != null && !tablesInclusionListMap.get(dbName).contains(
			                tableName)))
			{
				continue;
			}
			tableNameToSchemaMap.put(tableName, generateSchema(dbName, tableName));
		}
		return tableNameToSchemaMap;
	}

	/**
	 * Generate schema for the given table.
	 * @param dbName the db name
	 * @param tableName tableName
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String generateSchema(String dbName, String tableName) throws IOException, IllegalArgumentException
	{
		if ((tablesExclusionListMap.get(dbName) != null && tablesExclusionListMap.get(dbName).contains(tableName))
		        || (tablesInclusionListMap.get(dbName) != null && !tablesInclusionListMap.get(dbName).contains(
		                tableName)))
		{
			throw new IllegalArgumentException("table is excluded for the schema generation in the settings");
		}

		List<TableRecord.Field> fields = MysqlUtils.getFieldDetails(dbName, tableName);
		List<String> primaryKeys = MysqlUtils.getPrimaryKeys(dbName, tableName);

		String namespace = dbName;
		String doc =
		        "Auto-generated Avro schema for " + tableName + ". Generated at "
		                + df.format(new Date(System.currentTimeMillis()));

		TableRecord tableRecord;
		if (rowChangeFieldName != null)
		{
			SchemaGenerator.validateRowChangeField(tableName, namespace, fields, rowChangeFieldName);
			fields.add(new TableRecord.Field(rowChangeFieldName, "MAP", fields.size() + 1));
			tableRecord = new TableRecord(tableName, "record", doc, namespace, primaryKeys, fields, rowChangeFieldName);
		}
		else
		{
			tableRecord = new TableRecord(tableName, "record", doc, namespace, primaryKeys, fields);
		}

		/* mapping tableRecord to json */
		StringWriter writer = new StringWriter();
		JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
		jsonGenerator.useDefaultPrettyPrinter();
		objectMapper.writeValue(jsonGenerator, tableRecord);
		return writer.getBuffer().toString();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws PropertyVetoException
	{
		/** adding dataSource */
		for (DataSourceConfig dataSourceConfig : dataSourceConfigs)
		{
			MysqlConnectionProvider.getInstance().addDataSource(dataSourceConfig);
		}
	}

	/** Getter and setters */
	public List<DataSourceConfig> getDataSourceConfigs()
	{
		return dataSourceConfigs;
	}

	public void setDataSourceConfigs(List<DataSourceConfig> dataSourceConfigs)
	{
		this.dataSourceConfigs = dataSourceConfigs;
	}

	public Map<String, List<String>> getTablesInclusionListMap()
	{
		return tablesInclusionListMap;
	}

	public void setTablesInclusionListMap(Map<String, List<String>> tablesInclusionListMap)
	{
		this.tablesInclusionListMap = tablesInclusionListMap;
	}

	public Map<String, List<String>> getTablesExclusionListMap()
	{
		return tablesExclusionListMap;
	}

	public void setTablesExclusionListMap(Map<String, List<String>> tablesExclusionListMap)
	{
		this.tablesExclusionListMap = tablesExclusionListMap;
	}


	public String getRowChangeFieldName() {
		return rowChangeFieldName;
	}

	public void setRowChangeFieldName(String rowChangeFieldName) {
		this.rowChangeFieldName = rowChangeFieldName;
	}

	/**
	 * @param fields
	 * @param rowChangeFieldName
	 */
	private static void validateRowChangeField(String tableName, String namespace, List<TableRecord.Field> fields,
											   String rowChangeFieldName) throws IllegalArgumentException
	{
		for (TableRecord.Field field : fields)
		{
			if (field.getName().equals(rowChangeFieldName))
			{
				throw new IllegalArgumentException("FAILED: rowChangeFieldName: " + rowChangeFieldName + " clashes with orignal field: " +
						field.getName() + " (TableName:" + tableName + ",Namespace:" + namespace + ")");
			}

		}

	}

}
