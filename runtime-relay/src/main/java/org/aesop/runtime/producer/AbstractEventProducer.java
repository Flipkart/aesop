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
package org.aesop.runtime.producer;

import java.util.concurrent.atomic.AtomicLong;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;

public abstract class AbstractEventProducer {

	protected String name;
	protected AtomicLong sinceSCN = new AtomicLong(-1);
	protected DbusEventBufferAppendable eventBuffer;
	protected MaxSCNReaderWriter maxScnReaderWriter;
	
	/** Getter/Setter methods */		
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public AtomicLong getSinceSCN() {
		return sinceSCN;
	}	
	public void setSinceSCN(AtomicLong sinceSCN) {
		this.sinceSCN = sinceSCN;
	}
	public DbusEventBufferAppendable getEventBuffer() {
		return eventBuffer;
	}
	public void setEventBuffer(DbusEventBufferAppendable eventBuffer) {
		this.eventBuffer = eventBuffer;
	}
	public MaxSCNReaderWriter getMaxScnReaderWriter() {
		return maxScnReaderWriter;
	}
	public void setMaxScnReaderWriter(MaxSCNReaderWriter maxScnReaderWriter) {
		this.maxScnReaderWriter = maxScnReaderWriter;
	}
	
}
