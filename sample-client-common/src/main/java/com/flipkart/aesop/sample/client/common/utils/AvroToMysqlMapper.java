package com.flipkart.aesop.sample.client.common.utils;

import java.math.BigDecimal;

import org.apache.avro.util.Utf8;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

/**
 * <code>AvroToMysqlMapper</code> provides mapping of data from avro data type to mysql data type.
 * @author yogesh.dahiya
 */

public class AvroToMysqlMapper
{
	/** Logger for this class */
	private static final Logger LOGGER = LogFactory.getLogger(AvroToMysqlMapper.class);

	/**
	 * Provides mapping of data from avro to mysql
	 * @param Object value of avro data type
	 * @param MysqlType mysqlType to convert to
	 * @return returns the converted object
	 * @throws IllegalArgumentException for incompatible avro-mysql conversion
	 */
	public static Object avroToMysqlType(Object value, MysqlDataTypes mysqlType) throws IllegalArgumentException
	{
		if (value instanceof java.nio.ByteBuffer)
		{
			if (mysqlType == MysqlDataTypes.BIT || mysqlType == MysqlDataTypes.BLOB
			        || mysqlType == MysqlDataTypes.TINYBLOB || mysqlType == MysqlDataTypes.MEDIUMBLOB
			        || mysqlType == MysqlDataTypes.LONGBLOB || mysqlType == MysqlDataTypes.TINYTEXT
			        || mysqlType == MysqlDataTypes.MEDIUMTEXT || mysqlType == MysqlDataTypes.LONGTEXT
			        || mysqlType == MysqlDataTypes.TEXT)
			{
				return new String(((java.nio.ByteBuffer) value).array());
			}
			else
			{
				LOGGER.error("Incompatible types java.nio.ByteBuffer and " + mysqlType.toString());
				throw new IllegalArgumentException("Incompatible types java.nio.ByteBuffer and " + mysqlType.toString());
			}

		}
		else if (value instanceof Long)
		{
			if (mysqlType == MysqlDataTypes.INT || mysqlType == MysqlDataTypes.INTEGER
			        || mysqlType == MysqlDataTypes.TINYINT || mysqlType == MysqlDataTypes.MEDIUMINT
			        || mysqlType == MysqlDataTypes.BIGINT || mysqlType == MysqlDataTypes.SET)
			{
				return value;
			}
			else if (mysqlType == MysqlDataTypes.DATE)
			{
				return new java.sql.Date((Long) value);
			}
			else if (mysqlType == MysqlDataTypes.TIMESTAMP || mysqlType == MysqlDataTypes.DATETIME)
			{
				return new java.sql.Timestamp((Long) value);
			}
			else if (mysqlType == MysqlDataTypes.TIME)
			{
				return new java.sql.Time((Long) value);
			}
			else
			{
				LOGGER.error("Incompatible types Long and " + mysqlType.toString());
				throw new IllegalArgumentException("Incompatible types Long and " + mysqlType.toString());
			}

		}
		else if (value instanceof String)
		{
			if (mysqlType == MysqlDataTypes.CHAR || mysqlType == MysqlDataTypes.VARCHAR)
			{
				return value;
			}
			else if (mysqlType == MysqlDataTypes.DECIMAL)
			{
				return new BigDecimal((String) value);
			}
			else
			{
				LOGGER.error("Incompatible types String and " + mysqlType.toString());
				throw new IllegalArgumentException("Incompatible types String and " + mysqlType.toString());
			}
		}
		else if (value instanceof Utf8)
		{
			if (mysqlType == MysqlDataTypes.CHAR || mysqlType == MysqlDataTypes.VARCHAR)
			{
				return value.toString();
			}
			else if (mysqlType == MysqlDataTypes.DECIMAL)
			{
				return new BigDecimal(value.toString());
			}
			else
			{
				LOGGER.error("Incompatible types org.apache.avro.util.Utf8 and " + mysqlType.toString());
				throw new IllegalArgumentException("Incompatible types org.apache.avro.util.Utf8 and "
				        + mysqlType.toString());
			}
		}
		else if (value instanceof Double)
		{
			if (mysqlType == MysqlDataTypes.DOUBLE || mysqlType == MysqlDataTypes.FLOAT)
			{
				return value;
			}
			else
			{
				LOGGER.error("Incompatible types Double and " + mysqlType.toString());
				throw new IllegalArgumentException("Incompatible types Double and " + mysqlType.toString());
			}
		}
		else if (value instanceof Integer)
		{
			if (mysqlType == MysqlDataTypes.SMALLINT || mysqlType == MysqlDataTypes.TINYINT
			        || mysqlType == MysqlDataTypes.MEDIUMINT || mysqlType == MysqlDataTypes.INT
			        || mysqlType == MysqlDataTypes.INTEGER || mysqlType == MysqlDataTypes.ENUM
			        || mysqlType == MysqlDataTypes.YEAR)
			{
				return value;
			}
			else
			{
				LOGGER.error("Incompatible types Integer and " + mysqlType.toString());
				throw new IllegalArgumentException("Incompatible types Integer and " + mysqlType.toString());

			}

		}
		else if (value instanceof Float)
		{
			if (mysqlType == MysqlDataTypes.FLOAT)
			{
				return value;
			}
			else
			{
				LOGGER.error("Incompatible types Integer and " + mysqlType.toString());
				throw new IllegalArgumentException("Incompatible types Integer and " + mysqlType.toString());
			}
		}
		else if (value == null)
		{
			return null;
		}
		else
		{
			LOGGER.error("unsupported avro type ; avro type : " + value.getClass().toString());
			throw new IllegalArgumentException("unsupported avro type ; avro type : " + value.getClass().toString());
		}

	}

}
