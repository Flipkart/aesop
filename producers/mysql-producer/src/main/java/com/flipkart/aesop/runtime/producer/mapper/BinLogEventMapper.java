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
package com.flipkart.aesop.runtime.producer.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;

/**
 * <code>BinLogEventMapper</code> maps the specified bin log event details such as event header, mutated row and
 * operation code
 * to an appropriate instance of the {@link GenericRecord} sub-type T.
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public interface BinLogEventMapper<T extends GenericRecord>
{

	/**
	 * Maps the specified bin log event details such as event header, mutated row and operation code to an appropriate
	 * instance of the {@link GenericRecord} sub-type T
	 * @param columns list of columns of the mutated row.
	 */
	public T mapBinLogEvent(BinlogEventV4Header eventHeader, Row row, DbusOpcode dbusOpCode, Schema schema);

	/**
	 * Returns a name unique to this mapper type.
	 * @return unique name for this mapper type
	 */
	public String getUniqueName();

}
