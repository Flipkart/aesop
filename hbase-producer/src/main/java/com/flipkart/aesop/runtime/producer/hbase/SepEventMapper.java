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
package com.flipkart.aesop.runtime.producer.hbase;

import org.apache.avro.generic.GenericRecord;

import com.ngdata.sep.SepEvent;

/**
 * <code>SepEventMapper</code> maps a single {@link SepEvent} to an appropriate instance of the {@link GenericRecord} sub-type T.
 *
 * @author Regunath B
 * @version 1.0, 28 Jan 2014
 */
public interface SepEventMapper <T extends GenericRecord> {
	
	/**
	 * Maps the specified {@link SepEvent} to an appropriate instance of the {@link GenericRecord} sub-type T 
	 * @param event the SepEvent received from HBase WAL edits
	 * @return a mapped GenericRecord instance
	 */
	public T mapSepEvent(SepEvent event);
	
	/**
	 * Returns a name unique to this mapper type.
	 * @return unique name for this mapper type
	 */
	public String getUniqueName();

}
