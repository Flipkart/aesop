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

package com.flipkart.aesop.bootstrap.mysql.utils;

import java.nio.charset.Charset;
import java.sql.Time;
import java.util.Calendar;
import java.util.Date;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.bootstrap.mysql.MysqlEventProducer;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.BitColumn;
import com.google.code.or.common.glossary.column.BlobColumn;
import com.google.code.or.common.glossary.column.DateColumn;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.google.code.or.common.glossary.column.DecimalColumn;
import com.google.code.or.common.glossary.column.DoubleColumn;
import com.google.code.or.common.glossary.column.EnumColumn;
import com.google.code.or.common.glossary.column.FloatColumn;
import com.google.code.or.common.glossary.column.Int24Column;
import com.google.code.or.common.glossary.column.LongColumn;
import com.google.code.or.common.glossary.column.LongLongColumn;
import com.google.code.or.common.glossary.column.NullColumn;
import com.google.code.or.common.glossary.column.SetColumn;
import com.google.code.or.common.glossary.column.ShortColumn;
import com.google.code.or.common.glossary.column.StringColumn;
import com.google.code.or.common.glossary.column.TimeColumn;
import com.google.code.or.common.glossary.column.TimestampColumn;
import com.google.code.or.common.glossary.column.TinyColumn;
import com.google.code.or.common.glossary.column.YearColumn;

/**
 * <code>ORToMysqlMapper</code> provides mapping of data from open replicator data type to mysql data type
 * @author yogesh.dahiya
 */
public class ORToMysqlMapper
{
	public static final Logger LOGGER = LogFactory.getLogger(MysqlEventProducer.class);

	public Object orToMysqlType(Column column) throws Exception
	{
		if (column instanceof BitColumn)
		{
			// This is in byte order
			BitColumn byteColumn = (BitColumn) column;
			byte[] byteArray = byteColumn.getValue();
			return new String(byteArray);
		}
		else if (column instanceof BlobColumn)
		{
			BlobColumn blobColumn = (BlobColumn) column;
			byte[] byteArray = blobColumn.getValue();
			return new String(byteArray);
		}
		else if (column instanceof DateColumn)
		{
			DateColumn dateColumn = (DateColumn) column;
			Date date = dateColumn.getValue();
			return new java.sql.Date(date.getTime());
		}
		else if (column instanceof DatetimeColumn)
		{
			DatetimeColumn dateTimeColumn = (DatetimeColumn) column;
			Date date = dateTimeColumn.getValue();
			/**
			 * Bug in OR for DateTIme and Time data-types. MilliSeconds is not available for these columns but is set
			 * with currentMillis() wrongly.
			 */
			return new java.sql.Timestamp((date.getTime() / 1000) * 1000);
		}
		else if (column instanceof DecimalColumn)
		{
			DecimalColumn decimalColumn = (DecimalColumn) column;
			return decimalColumn.getValue();
		}
		else if (column instanceof DoubleColumn)
		{
			DoubleColumn doubleColumn = (DoubleColumn) column;
			return doubleColumn.getValue();
		}
		else if (column instanceof EnumColumn)
		{
			EnumColumn enumColumn = (EnumColumn) column;
			return enumColumn.getValue();
		}
		else if (column instanceof FloatColumn)
		{
			FloatColumn floatColumn = (FloatColumn) column;
			return floatColumn.getValue();
		}
		else if (column instanceof Int24Column)
		{
			Int24Column intColumn = (Int24Column) column;
			return intColumn.getValue();
		}
		else if (column instanceof LongColumn)
		{
			LongColumn longColumn = (LongColumn) column;
			return longColumn.getValue();
		}
		else if (column instanceof LongLongColumn)
		{
			LongLongColumn longLongColumn = (LongLongColumn) column;
			return longLongColumn.getValue();
		}
		else if (column instanceof NullColumn)
		{
			return null;
		}
		else if (column instanceof SetColumn)
		{
			SetColumn setColumn = (SetColumn) column;
			return setColumn.getValue();
		}
		else if (column instanceof ShortColumn)
		{
			ShortColumn shortColumn = (ShortColumn) column;
			return shortColumn.getValue();
		}
		else if (column instanceof StringColumn)
		{
			StringColumn stringColumn = (StringColumn) column;
			return new String(stringColumn.getValue(), Charset.defaultCharset());
		}
		else if (column instanceof TimeColumn)
		{
			TimeColumn timeColumn = (TimeColumn) column;
			Time time = timeColumn.getValue();
			/**
			 * There is a bug in OR where instead of using the default year as 1970, it is using 0070.
			 * This is a temporary measure to resolve it by working around at this layer. The value obtained from OR is
			 * subtracted from "0070-00-01 00:00:00"
			 */
			Calendar c = Calendar.getInstance();
			c.set(70, 0, 1, 0, 0, 0);
			/**
			 * round off the milli-seconds as TimeColumn type has only seconds granularity but Calendar implementation
			 * includes milli-second (System.currentTimeMillis() at the time of instantiation)
			 */
			long rawVal = (c.getTimeInMillis() / 1000) * 1000;
			long val2 = (time.getTime() / 1000) * 1000;
			return new java.sql.Time(val2 - rawVal);
		}
		else if (column instanceof TimestampColumn)
		{
			TimestampColumn timeStampColumn = (TimestampColumn) column;
			return timeStampColumn.getValue();
		}
		else if (column instanceof TinyColumn)
		{
			TinyColumn tinyColumn = (TinyColumn) column;
			return tinyColumn.getValue();
		}
		else if (column instanceof YearColumn)
		{
			YearColumn yearColumn = (YearColumn) column;
			return yearColumn.getValue();
		}
		else
		{
			String message = "Unknown MySQL type in the event" + column.getClass() + " Object = " + column;
			LOGGER.error(message);
			throw new RuntimeException(message);
		}
	}
}
