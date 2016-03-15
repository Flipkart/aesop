package com.flipkart.aesop.avro.schemagenerator.data;

import java.lang.Object;
import java.util.Arrays;
import java.util.HashMap;

/**
 * <code> MysqlToAvroMapper </code> maps Mysql data type to Avro data types.
 * @author chandan.bansal
 */
public enum MysqlToAvroMapper
{

	/** The int. */
	INT("int"),

	/** The integer. */
	INTEGER("int"),

	/** The tinyint. */
	TINYINT("int"),

	/** The smallint. */
	SMALLINT("int"),

	/** The mediumint. */
	MEDIUMINT("int"),

	/** The bigint. */
	BIGINT("long"),

	/** The float. */
	FLOAT("float"),

	/** The double. */
	DOUBLE("double"),

	/** The decimal. */
	DECIMAL("string"),

	/** The bit. */
	BIT("bytes"),

	/** String types. */

	CHAR("string"),

	/** The varchar. */
	VARCHAR("string"),

	/** The binary. */
	BINARY("binary"),

	/** The varbinary. */
	VARBINARY("binary"),

	/** The text. */
	TEXT("bytes"),

	/** The tinytext. */
	TINYTEXT("bytes"),

	/** The mediumtext. */
	MEDIUMTEXT("bytes"),

	/** The longtext. */
	LONGTEXT("bytes"),

	/** The blob. */
	BLOB("bytes"),

	/** The tinyblob. */
	TINYBLOB("bytes"),

	/** The mediumblob. */
	MEDIUMBLOB("bytes"),

	/** The longblob. */
	LONGBLOB("bytes"),

	/** The enum. */
	ENUM("int"),

	/** The set. */
	SET("long"),

	/** Date and Time Types. */

	DATE("long"),

	/** The datetime. */
	DATETIME("long"),

	/** The timestamp. */
	TIMESTAMP("long"),

	/** The time. */
	TIME("long"),

	/** The year. */
	YEAR("long"),

	/** For HashMap.
	 * Though this Mysql Datatype does exist, this required for
	 * passing record changes in form of Avro Map datatype.
	 */
	MAP(new HashMap<String, Object>(){{
	        put("type", "map");
			put("values", Arrays.asList("int", "long", "float", "double", "bytes", "string", "null"));
	    }});


	/** The avro type. */
	private final Object avroType;

	/**
	 * enum constructor.
	 * @param avroType the avro data type
	 */
	private MysqlToAvroMapper(Object avroType)
	{
		this.avroType = avroType;
	}

	/**
	 * Gets the avro type.
	 * @return the avro type
	 */
	public Object getAvroType()
	{
		return avroType;
	}

}
