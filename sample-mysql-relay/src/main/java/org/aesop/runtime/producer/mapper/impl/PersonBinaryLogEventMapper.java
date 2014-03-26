/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aesop.runtime.producer.mapper.impl;

import java.util.List;

import org.aesop.events.ortest.Person;
import org.aesop.runtime.producer.mapper.BinLogEventMapper;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;

/**
 * <code>PersonBinaryLogEventMapper</code> is an implementation of the {@link BinLogEventMapper} that creates instance of the type {@link Person} from Bin log events
 *
 * @author Shoury B
 * @version 1.0, 26 Mar 2014
 */
public class PersonBinaryLogEventMapper implements BinLogEventMapper<org.aesop.events.ortest.Person> {

	/** Logger for this class*/
	protected static final Logger LOGGER = LogFactory.getLogger(PersonBinaryLogEventMapper.class);
	
	/** Open replicator to Avro mapper */
	public ORToAvroMapper orToAvroMapper;
	

	/**
	 * Interface method implementation. Returns the name of this type
	 * @see org.aesop.runtime.producer.mapper.BinLogEventMapper#getUniqueName()
	 */
	public String getUniqueName() {
		return this.getClass().getCanonicalName();
	}

	/**
	 * Interface method implementation. Creates {@link Person} instance based on data in {@link Row} ,{@link BinlogEventV4Header} and {@link DbusOpcode} 
	 * @see org.aesop.runtime.producer.mapper.BinLogEventMapper#mapBinLogEvent(com.google.code.or.binlog.BinlogEventV4Header,com.google.code.or.common.glossary.Row,com.linkedin.databus.core.DbusOpcode)
	 */
	@Override
	public Person mapBinLogEvent(BinlogEventV4Header header, Row row,
			DbusOpcode databusCode) {
		List<Column> columns = row.getColumns();
		try
		{
			Person person =  Person.newBuilder().setKey((Long)orToAvroMapper.orToAvroType(columns.get(0))).
					setFirstName((CharSequence)orToAvroMapper.orToAvroType(columns.get(1))).
					setLastName((CharSequence)orToAvroMapper.orToAvroType(columns.get(2))).
					setBirthDate((Long)orToAvroMapper.orToAvroType(columns.get(3))).
					setDeleted((CharSequence)orToAvroMapper.orToAvroType(columns.get(4)))
					.build();
			LOGGER.info("Mapped Person : " + person);
			return person;
		}catch (Exception e) {
			LOGGER.error("Error while mapping to person . Exception : " + e.getMessage() + " Cause: " + e.getCause());
		}
		return null;
	}

	/**Getters/Setters*/
	public ORToAvroMapper getOrToAvroMapper() {
		return orToAvroMapper;
	}
	public void setOrToAvroMapper(ORToAvroMapper orToAvroMapper) {
		this.orToAvroMapper = orToAvroMapper;
	}
}
