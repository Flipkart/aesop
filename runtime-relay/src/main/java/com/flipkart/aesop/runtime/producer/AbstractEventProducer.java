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

import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * <code>AbstractEventProducer</code> is an implementation of {@link EventProducer} that provides convenience methods for all sub-types
 *
 * @author Regunath B
 * @version 1.0, 17 Jan 2014
 */

public abstract class AbstractEventProducer implements EventProducer {

	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(AbstractEventProducer.class);
	
	/** Name of this event producer*/
	protected String name;
	
	/** Source related member variables*/
	protected PhysicalSourceConfig physicalSourceConfig;
	protected PhysicalSourceStaticConfig physicalSourceStaticConfig;
    protected SchemaRegistryService schemaRegistryService; 	
	/** Event-handling related member variables*/
	protected AtomicLong sinceSCN = new AtomicLong(-1); // This is the previous SCN to which it was read. 
	protected DbusEventBufferAppendable eventBuffer;
	protected MaxSCNReaderWriter maxScnReaderWriter;
	protected DbusEventsStatisticsCollector dbusEventsStatisticsCollector;	
	
	/** Avro serialization related member variables*/
	protected EncoderFactory factory = EncoderFactory.get();
	protected BinaryEncoder cachedAvroEncoder;	
	
	/**
	 * Serializes the specified Avro record into a byte array
	 * @param record the Avrop record
	 * @return byte array containing serialized form of the specified Avro record
	 */
	protected byte[] serializeEvent(GenericRecord record) {
		byte[] serializedValue;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			Encoder encoder = factory.directBinaryEncoder(bos,cachedAvroEncoder);
			GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
			writer.write(record, encoder);
			serializedValue = bos.toByteArray();
			return serializedValue;
		} catch (Exception ex) {
			LOGGER.error("Error serializing Avro object : " + record.getSchema(), ex);
		}
		return null;
	}

	/**
	 * Interface method implementation. Invokes rollback on the event buffer
	 * @see com.linkedin.databus2.producers.EventProducer#shutdown()
	 */
    public void shutdown() {
		eventBuffer.rollbackEvents();
    }

	/** Getter/Setter methods */	
	public String getName() {
		return this.name;
	}
	public long getSCN() {
		return this.sinceSCN.get();
	}
	public void setSchemaRegistryService(SchemaRegistryService schemaRegistryService) throws Exception {
		this.schemaRegistryService = schemaRegistryService;
		this.physicalSourceStaticConfig = this.physicalSourceConfig.build();
	}		
	public void setDbusEventsStatisticsCollector(DbusEventsStatisticsCollector dbusEventsStatisticsCollector) {
		this.dbusEventsStatisticsCollector = dbusEventsStatisticsCollector;
	}
	public PhysicalSourceConfig getPhysicalSourceConfig() {
		return physicalSourceConfig;
	}
	public void setPhysicalSourceConfig(PhysicalSourceConfig physicalSourceConfig) {
		this.physicalSourceConfig = physicalSourceConfig;		
		this.name = this.physicalSourceConfig.getName();
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
	public PhysicalSourceStaticConfig getPhysicalSourceStaticConfig() {
		return physicalSourceStaticConfig;
	}

}
