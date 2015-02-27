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
package com.flipkart.aesop.runtime.producer;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

/**
 * The <code>ReadEventCycleSummary</code> is a container for change events from a read cycle.
 * 
 * @author Regunath B
 * @version 1.0, 19 March 2014
 */

public class ReadEventCycleSummary<S extends GenericRecord> {

	/** Members variables containing results of event production cycle*/
	private long sinceSCN;
	private List<S> changeEvents;
	
	/** Constructor with member variables*/
	public ReadEventCycleSummary(List<S> changeEvents, long sinceSCN) {
		this.changeEvents = changeEvents;
		this.sinceSCN = sinceSCN;
	}

	/** Member variable getter methods*/
	public long getSinceSCN() {
		return sinceSCN;
	}
	public List<S> getChangeEvents() {
		return changeEvents;
	}
		
}
