package com.flipkart.aesop.runtime.producer.mapper.impl;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

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
import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus2.core.DatabusException;

public enum ORToAvroMapper {
	instance;

	public  Object orToAvroType(Column s)
		      throws DatabusException
		  {
		    if (s instanceof BitColumn)
		    {
		      // This is in  byte order
		      BitColumn bc = (BitColumn) s;
		      byte[] ba = bc.getValue();
		      ByteBuffer b = ByteBuffer.wrap(ba);
		      return b;
		    }
		    else if (s instanceof BlobColumn)
		    {
		      BlobColumn bc = (BlobColumn) s;
		      byte[] ba = bc.getValue();
		      return ByteBuffer.wrap(ba);
		    }
		    else if (s instanceof DateColumn)
		    {
		      DateColumn dc =  (DateColumn) s;
		      Date d = dc.getValue();
		      Long l = d.getTime();
		      return l;
		    }
		    else if (s instanceof DatetimeColumn)
		    {
		      DatetimeColumn dc = (DatetimeColumn) s;
		      Date d = dc.getValue();
		      Long t1 = (d.getTime()/1000) * 1000; //Bug in OR for DateTIme and Time data-types. MilliSeconds is not available for these columns but is set with currentMillis() wrongly.
		      return t1;
		    }
		    else if (s instanceof DecimalColumn)
		    {
		      DecimalColumn dc = (DecimalColumn) s;
		      String s1 = dc.getValue().toString(); // Convert to string for preserving precision
		      return s1;
		    }
		    else if (s instanceof DoubleColumn)
		    {
		      DoubleColumn dc = (DoubleColumn) s;
		      Double d = dc.getValue();
		      return d;
		    }
		    else if (s instanceof EnumColumn)
		    {
		      EnumColumn ec = (EnumColumn) s;
		      Integer i = ec.getValue();
		      return i;
		    }
		    else if (s instanceof FloatColumn)
		    {
		      FloatColumn fc = (FloatColumn) s;
		      Float f = fc.getValue();
		      return f;
		    }
		    else if (s instanceof Int24Column)
		    {
		      Int24Column ic = (Int24Column) s;
		      Integer i = ic.getValue();
		      return i;
		    }
		    else if (s instanceof LongColumn)
		    {
		      LongColumn lc = (LongColumn) s;
		      Integer i = lc.getValue();
		      return i;
		    }
		    else if (s instanceof LongLongColumn)
		    {
		      LongLongColumn llc = (LongLongColumn) s;
		      Long l = llc.getValue();
		      return l;
		    }
		    else if (s instanceof NullColumn)
		    {
		      return null;
		    }
		    else if (s instanceof SetColumn)
		    {
		      SetColumn sc = (SetColumn) s;
		      Long l = sc.getValue();
		      return l;
		    }
		    else if (s instanceof ShortColumn)
		    {
		      ShortColumn sc = (ShortColumn) s;
		      Integer i = sc.getValue();
		      return i;
		    }
		    else if (s instanceof StringColumn)
		    {
		      StringColumn sc = (StringColumn) s;
		      String str = new String(sc.getValue(), Charset.defaultCharset());
		      return str;
		    }
		    else if (s instanceof TimeColumn)
		    {
		      TimeColumn tc = (TimeColumn) s;
		      Time t = tc.getValue();
		      /**
		       * There is a bug in OR where instead of using the default year as 1970, it is using 0070.
		       * This is a temporary measure to resolve it by working around at this layer. The value obtained from OR is subtracted from "0070-00-01 00:00:00"
		       */
		      Calendar c = Calendar.getInstance();
		      c.set(70, 0, 1, 0, 0, 0);
		      // round off the milli-seconds as TimeColumn type has only seconds granularity but Calendar implementation
		      // includes milli-second (System.currentTimeMillis() at the time of instantiation)
		      long rawVal = (c.getTimeInMillis()/1000) * 1000;
		      long val2 = (t.getTime()/1000) * 1000;
		      long offset = val2 - rawVal;
		      return offset;
		    }
		    else if (s instanceof TimestampColumn)
		    {
		      TimestampColumn tsc = (TimestampColumn) s;
		      Timestamp ts = tsc.getValue();
		      Long t = ts.getTime();
		      return t;
		    }
		    else if (s instanceof TinyColumn)
		    {
		      TinyColumn tc = (TinyColumn) s;
		      Integer i = tc.getValue();
		      return i;
		    }
		    else if (s instanceof YearColumn)
		    {
		      YearColumn yc = (YearColumn) s;
		      Integer i = yc.getValue();
		      return i;
		    }
		    else
		    {
		      throw new DatabusRuntimeException("Unknown MySQL type in the event" + s.getClass() + " Object = " + s);
		    }
		  }

}
