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
package com.flipkart.aesop.relay.sample;

import java.util.LinkedList;

import com.flipkart.aesop.events.sample.person.FieldChange;
import com.flipkart.aesop.events.sample.person.Person;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.producer.hbase.SepEventMapper;
import com.ngdata.sep.SepEvent;


/**
 * <code>PersonSepEventMapper</code> is an implementation of the {@link SepEventMapper} that creates instance of the type {@link Person} from HBase WAL edits
 *
 * @author Regunath B
 * @version 1.0, 28 Jan 2014
 */

public class PersonSepEventMapper implements SepEventMapper<Person> {
	
	/** Logger for this class*/
	protected static final Logger LOGGER = LogFactory.getLogger(PersonSepEventMapper.class);
	
	/**
	 * Interface method implementation. Returns the name of this type
	 * @see com.flipkart.aesop.runtime.producer.hbase.SepEventMapper#getUniqueName()
	 */
	public String getUniqueName() {
		return this.getClass().getCanonicalName();
	}

	/**
	 * Interface method implementation. Creates {@link Person} instance based on data in {@link KeyValue} contained in {@link SepEvent}
	 * @see com.flipkart.aesop.runtime.producer.hbase.SepEventMapper#mapSepEvent(com.ngdata.sep.SepEvent)
	 */
	public Person mapSepEvent(SepEvent sepEvent) {
    	String firstName = null;
    	String lastName = null;
    	long dob = 0;
    	String deleted = "false";
        for (KeyValue kv : sepEvent.getKeyValues()) {
        	if (kv.isDeleteFamily()) {
                LOGGER.debug("Returning Delete Person object : " + sepEvent.getRow() + " " + deleted);        
        		return new Person(Bytes.toLong(sepEvent.getRow()), "","",0L,"true",new LinkedList<FieldChange>());
        	} else {
				String columnQualifier = new String(kv.getQualifier());	
				if (columnQualifier.equalsIgnoreCase("firstName")) {
					firstName = Bytes.toString(kv.getValue());
				} else if (columnQualifier.equalsIgnoreCase("lastName")) {
					lastName = Bytes.toString(kv.getValue());						
				} else if (columnQualifier.equalsIgnoreCase("birthDate")) {
					dob = Bytes.toLong(kv.getValue());
				}
        	}
        }
        LOGGER.debug("Returning Person object : " + sepEvent.getRow() + " " + firstName + " " + lastName);        
        return new Person(Bytes.toLong(sepEvent.getRow()),firstName, lastName, dob, deleted,new LinkedList<FieldChange>());
	}

}
