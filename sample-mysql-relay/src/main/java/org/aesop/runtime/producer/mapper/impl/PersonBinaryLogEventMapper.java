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
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.mapper.BinLogEventMapper;
import com.google.code.or.binlog.BinlogEventV4Header;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;
import com.linkedin.databus.core.DbusOpcode;


public class PersonBinaryLogEventMapper implements BinLogEventMapper<org.aesop.events.ortest.Person> {

	/** Logger for this class*/
	protected static final Logger LOGGER = LogFactory.getLogger(PersonBinaryLogEventMapper.class);

	/**
	 * Interface method implementation. Returns the name of this type
	 * @see org.aesop.runtime.producer.hbase.SepEventMapper#getUniqueName()
	 */
	public String getUniqueName() {
		return this.getClass().getCanonicalName();
	}

	@Override
	public Person mapBinLogEvent(BinlogEventV4Header header, Row row,
			DbusOpcode databusCode) {
		List<Column> columns = row.getColumns();
		try
		{
			Person person =  Person.newBuilder().setKey((Long)ORToAvroMapper.instance.orToAvroType(columns.get(0))).
					setFirstName((CharSequence)ORToAvroMapper.instance.orToAvroType(columns.get(1))).
					setLastName((CharSequence)ORToAvroMapper.instance.orToAvroType(columns.get(2))).
					setBirthDate((Long)ORToAvroMapper.instance.orToAvroType(columns.get(3))).
					setDeleted((CharSequence)ORToAvroMapper.instance.orToAvroType(columns.get(4)))
					.build();
			LOGGER.info("Mapped Person : " + person);
			return person;
		}catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}


}
