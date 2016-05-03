package com.flipkart.aesop.avro.schemagenerator.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.flipkart.aesop.avro.schemagenerator.data.TableRecord;

/**
 * <code> MysqlUtils </code> utils to fetch info from mysql database
 * @author chandan.bansal
 */
public class MysqlUtils
{
	/** Fields fetch query */
	private static final String FIELDS_FETCH_QUERY =
	        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?";
	/** Tables fetch query */
	private static final String TABLES_FETCH_QUERY =
	        "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?";
	/** Table check query */
	private static final String TABLE_CHECK_QUERY = "SELECT count(*) AS count FROM ";
	/** Field details fetch query */
	private static final String FIELDS_DETAILS_FETCH_QUERY =
	        "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?";

	/**
	 * Release db resource.
	 * @param connection object
	 * @param resultSet Object
	 * @param statement Object
	 */
	private static void releaseDBResource(Connection connection, ResultSet resultSet, Statement statement)
	{
		try
		{
			if (connection != null)
			{
				connection.close();
			}

		}
		catch (Exception e)
		{

		}
		try
		{
			if (resultSet != null)
				resultSet.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		try
		{
			if (statement != null)
				statement.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Get the tables in the current database.
	 * @param dbName the db name
	 * @return list of tables in the current db
	 */
	public static List<String> getTablesInDB(String dbName)
	{
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		ArrayList<String> list = new ArrayList<String>();
		try
		{
			connection = MysqlConnectionProvider.getInstance().getConnection(dbName);
			preparedStatement = connection.prepareStatement(TABLES_FETCH_QUERY);
			preparedStatement.setString(1, dbName);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next())
			{
				list.add(resultSet.getString("table_name"));
			}
		}
		catch (SQLException e)
		{
			System.out.println("ERROR: Unable to fetch the table names in the database specified: " + e.toString());
		}
		finally
		{
			releaseDBResource(connection, resultSet, preparedStatement);
		}

		return list;
	}

	/**
	 * Gets the primary keys.
	 * @param dbName the dataSourceId
	 * @param tableName the table name
	 * @return the primary keys
	 */
	public static List<String> getPrimaryKeys(String dbName, String tableName)
	{
		List<String> primaryKeyList = new ArrayList<String>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try
		{
			connection = MysqlConnectionProvider.getInstance().getConnection(dbName);
			String primaryKeyFetchQuery = "SHOW index FROM " + tableName + " WHERE Key_name = 'PRIMARY'";
			preparedStatement = connection.prepareStatement(primaryKeyFetchQuery);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next())
			{
				primaryKeyList.add(resultSet.getString("Column_name"));
			}

		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			releaseDBResource(connection, resultSet, preparedStatement);
		}
		return primaryKeyList;
	}

	/**
	 * Gets the fields in table.
	 * @param db the database
	 * @param table the table
	 * @return the fields in table
	 */
	public static List<String> getFieldsInTable(String db, String table)
	{
		ArrayList<String> list = new ArrayList<String>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try
		{
			connection = MysqlConnectionProvider.getInstance().getConnection(db);
			preparedStatement = connection.prepareStatement(FIELDS_FETCH_QUERY);
			preparedStatement.setString(1, db);
			preparedStatement.setString(2, table);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next())
				list.add(resultSet.getString("COLUMN_NAME"));
		}
		catch (SQLException e)
		{
			System.out.println("Unable to determine the fields from the given table: " + e.toString());
		}
		finally
		{
			releaseDBResource(connection, resultSet, preparedStatement);
		}
		return list;
	}

	/**
	 * checks if the current table is a valid table in the given schema.
	 * @param dataBase the dataSourceId
	 * @param table : table name
	 * @return true if valid table, false otherwise
	 */
	public static boolean isValidTable(String dataBase, String table)
	{
		Connection connection = null;
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;

		try
		{
			connection = MysqlConnectionProvider.getInstance().getConnection(dataBase);
			preparedStatement = connection.prepareStatement(TABLE_CHECK_QUERY + table);

			resultSet = preparedStatement.executeQuery();
			if (resultSet.next() && resultSet.getInt("count") >= 0)
			{
				return true;
			}
		}
		catch (SQLException e)
		{
			System.out.println("ERROR: Unable to determine if it's a valid schema : " + e.toString());
			return false;
		}
		finally
		{
			releaseDBResource(connection, resultSet, preparedStatement);
		}

		return true;
	}

	/**
	 * Checks if the field is present in the table.
	 * @param database the database
	 * @param field The field to check if it's valid
	 * @param table the table
	 * @return true, if is valid field
	 */
	public static boolean isValidField(String database, String field, String table)
	{
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try
		{
			connection = MysqlConnectionProvider.getInstance().getConnection(database);

			preparedStatement =
			        connection
			                .prepareStatement("SELECT count(COLUMN_NAME)  as count FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? and COLUMN_NAME = ?");
			preparedStatement.setString(1, database);
			preparedStatement.setString(2, table);
			preparedStatement.setString(3, field);
			resultSet = preparedStatement.executeQuery();
			return (resultSet.next() && resultSet.getInt("count") > 0);
		}
		catch (SQLException e)
		{
			System.out.println("ERROR: Unable to determine if it's a valid field ( " + field + "): " + e.toString());
			return false;
		}
		finally
		{
			releaseDBResource(connection, resultSet, preparedStatement);
		}
	}

	/**
	 * Gets the field details.
	 * @param db the db name
	 * @param table the table name
	 * @return the field details
	 */
	public static List<TableRecord.Field> getFieldDetails(String db, String table)
	{
		List<TableRecord.Field> fieldInfoList = new ArrayList<TableRecord.Field>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try
		{
			connection = MysqlConnectionProvider.getInstance().getConnection(db);
			preparedStatement = connection.prepareStatement(FIELDS_DETAILS_FETCH_QUERY);
			preparedStatement.setString(1, db);
			preparedStatement.setString(2, table);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next())
			{
				fieldInfoList.add(new TableRecord.Field(resultSet.getString("COLUMN_NAME"), resultSet
				        .getString("DATA_TYPE"), resultSet.getInt("ORDINAL_POSITION")));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			releaseDBResource(connection, resultSet, preparedStatement);
		}
		return fieldInfoList;
	}

}
