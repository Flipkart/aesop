package com.flipkart.aesop.avro.schemagenerator.main;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.flipkart.aesop.avro.schemagenerator.main.common.JLineHelper;
import com.flipkart.aesop.avro.schemagenerator.mysql.DataSourceConfig;
import com.flipkart.aesop.avro.schemagenerator.mysql.MysqlUtils;

/**
 * The Class InteractiveSchemaGenerator.
 * @author chandan.bansal
 */
public class InteractiveSchemaGenerator
{

	/** The current reader the console reader is at. */
	private State currentState;
	/** The _verbose. */
	boolean _verbose = false;
	/** The _database. */
	String _database;
	/** The _databasehost. */
	String _databasehost = "localhost";
	/** The _user name. */
	String _userName = "root";
	/** The _password. */
	String _password;
	/** The _table names. */
	List<String> _tableNames;
	/** The primary keys for the table. */
	List<String> _primaryKeys = null;
	/** The _user fields. */
	List<String> _userFields = null;
	/** The jline obj. */
	JLineHelper jlineObj;
	/** SchemaGenerator instance */
	SchemaGenerator schemaGenerator;

	// The current state of the console reader
	/**
	 * The Enum State.
	 * @author chandan.bansal
	 */
	private enum State
	{

		/** The welcome. */
		WELCOME,
		/** The initiateconn. */
		INITIATECONN,
		/** The tableinfo. */
		TABLEINFO,
		/** The schema manipulation. */
		SCHEMA_MANIPULATION,
		/** The generate relay src configs. */
		GENERATE_RELAY_SRC_CONFIGS,
		/** The exit. */
		EXIT
	}

	/**
	 * Instantiates a new mysql avro converter.
	 */
	public InteractiveSchemaGenerator()
	{
		currentState = State.WELCOME;
		_verbose = true;
	}

	/**
	 * The main method.
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception
	{
		InteractiveSchemaGenerator obj = new InteractiveSchemaGenerator();
		obj.run();
	}

	/**
	 * Run.
	 * @throws Exception the exception
	 */
	public void run() throws Exception
	{
		printWelcomeMessage();
		jlineObj = new JLineHelper("SchemaGen>");
		processInput();

	}

	/**
	 * prints the welcome message.
	 */
	private void printWelcomeMessage()
	{
		System.out.println("Welcome to AVRO schema generation tool.");
	}

	/**
	 * Reads the user input from the console reader and processes.
	 * @throws Exception the exception
	 */
	private void processInput() throws Exception
	{
		boolean done = false;
		while (!done)
		{
			switch (currentState)
			{
				case WELCOME :
				{
					System.out
					        .println("This tools will only work for Mysql to get Avro schemas. Press enter to continue...");
					String line = jlineObj.checkAndRead();
					if (!line.isEmpty())
					{
						System.out.println("Wrong input.");
						continue;
					}
					currentState = State.INITIATECONN;
				}
				case INITIATECONN :
				{
					System.out.println("Enter the Databse Name:");
					_database = jlineObj.checkAndRead();
					System.out.println("Enter the Databse host:");
					_databasehost = jlineObj.checkAndRead();
					System.out.println("Enter the Username for the Database:");
					_userName = jlineObj.checkAndRead();
					System.out.println("Enter the password for the database:");
					_password = jlineObj.checkAndRead();
					System.out.println("Attempting to connect with the database..");
					currentState = State.TABLEINFO;
					initilizeDB(_database, _databasehost, _userName, _password);

				}
					break;
				case TABLEINFO :
				{
					List<String> tablesList = null;
					tablesList = MysqlUtils.getTablesInDB(_database);
					jlineObj.addListToCompletor(tablesList);
					System.out
					        .println("Enter the name of table you would like to generate the schema for (use tab to autocomplete table names), Hit enter to generate for all tables...");

					String line = jlineObj.checkAndRead();
					if (line == null || line.isEmpty())
					{
						_tableNames = tablesList;
					}
					else
					{
						_tableNames = new ArrayList<String>(Arrays.asList(line.split(",")));
						if (!areTablesValid())
						{
							continue;
						}
					}
					System.out.println("Generating schema for [" + _tableNames + "]");
					jlineObj.removeCurrentCompletor();
					currentState = State.SCHEMA_MANIPULATION;
				}
					break;
				case SCHEMA_MANIPULATION :
				{
					runSchemaGenTool();
					currentState = State.EXIT;
				}
					break;
				case EXIT :
					System.out.println("Schema generation is complete");
					done = true;
					break;
				default :
					throw new Exception("Undefined state!");
			}

		}
	}

	/**
	 * Run schema gen tool.
	 * @return true, if successful
	 */
	public boolean runSchemaGenTool()
	{
		for (String table : _tableNames)
		{

			try
			{
				System.out.println(schemaGenerator.generateSchema(_database, table));
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}

		return true;
	}

	/**
	 * Convert a list of comma separated of fields to list of fields.
	 * @param database the database
	 * @param fieldList The comma separated list of fields
	 * @param table the table
	 * @return A list of primary keys, null if the fieldList has an invalid field
	 */
	@SuppressWarnings ("unused")
	private List<String> fieldToList(String database, String fieldList, String table)
	{
		String[] fieldArray = fieldList.split(",");
		ArrayList<String> fieldArrayList = new ArrayList<String>();

		if (fieldList.length() == 0)
			return null;

		for (String field : fieldArray)
		{
			if (!MysqlUtils.isValidField(_database, field.trim(), table))
			{
				System.out.println("The field " + field + " is not a valid field in the table, please retry");
				return null;
			}

			fieldArrayList.add(field.trim().toUpperCase(Locale.ENGLISH));
		}

		return fieldArrayList;
	}

	/**
	 * Initilize db.
	 * @param _database the _database
	 * @param _databasehost the _databasehost
	 * @param _userName the _user name
	 * @param _password the _password
	 */
	private void initilizeDB(String _database, String _databasehost, String _userName, String _password)
	{
		DataSourceConfig dataSourceConfig = new DataSourceConfig();
		dataSourceConfig.setDbName(_database);
		dataSourceConfig.setHostName(_databasehost);
		dataSourceConfig.setUserName(_userName);
		dataSourceConfig.setPassword(_password);

		List<DataSourceConfig> dataSourceConfigs = new ArrayList<DataSourceConfig>();
		dataSourceConfigs.add(dataSourceConfig);
		try
		{
			this.schemaGenerator = new SchemaGenerator(dataSourceConfigs);

		}
		catch (PropertyVetoException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Are tables valid.
	 * @return true, if successful
	 */
	private boolean areTablesValid()
	{
		for (String table : _tableNames)
		{
			if (!MysqlUtils.isValidTable(_database, table))
			{
				System.out.println("This table [" + table + "] doesn't appear to be valid table or view, please retry");
				return false;
			}
		}
		return true;
	}
}
