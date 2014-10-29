package com.flipkart.aesop.sample.client.common.utils;

/**
 * <code> MysqlDataType </code> enum
 * @author yogesh.dahiya
 */

public enum MysqlDataTypes
{
	/** Numeric Types */
	INT,
	INTEGER,
	SMALLINT,
	TINYINT,
	MEDIUMINT,
	BIGINT,
	FLOAT,
	DOUBLE,
	DECIMAL,
	BIT,

	/** Date and Time Types */
	DATE,
	DATETIME,
	TIMESTAMP,
	TIME,
	YEAR,

	/** String types */
	CHAR,
	VARCHAR,
	BINARY,
	VARBINARY,
	TEXT,
	TINYTEXT,
	MEDIUMTEXT,
	LONGTEXT,
	BLOB,
	TINYBLOB,
	MEDIUMBLOB,
	LONGBLOB,
	ENUM,
	SET;

}
