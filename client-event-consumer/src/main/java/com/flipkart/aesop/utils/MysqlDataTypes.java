/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 *******************************************************************************/

package com.flipkart.aesop.utils;

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
