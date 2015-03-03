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
import java.util.concurrent.atomic.AtomicBoolean;

import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.events.sample.person.FieldChange;
import com.flipkart.aesop.events.sample.person.Person;
import com.flipkart.aesop.runtime.producer.AbstractEventProducer;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * <code>PersonEventProducer</code> is a sub-type of {@link AbstractEventProducer}} that creates a specified number 
 * of change events of type {@link Person} using in-memory data. The events are created in a separate thread and appended to the Databus event buffer instance.
 *
 * @author Regunath B
 * @version 1.0, 17 Jan 2014
 */
public class PersonEventProducer extends AbstractEventProducer {

	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(PersonEventProducer.class);

	/** The default number of events to produce in a single run*/
	private static final int NUM_EVENTS = 10;
	
	/** Member variables related to events production an handling*/
	private int numberOfEventsPerRun = NUM_EVENTS;
	
	private volatile AtomicBoolean shutdownRequested = new AtomicBoolean(false);

	/**
	 * Interface method implementation
	 * @see com.linkedin.databus2.producers.EventProducer#getName()
	 */
	public String getName() {
		return "PersonEventProducer";
	}
	
	/**
	 * Interface method implementation. Starts up the event producer thread
	 * @see com.linkedin.databus2.producers.EventProducer#start(long)
	 */
	public void start (long sinceSCN) 
	{
		shutdownRequested.set(false);
		this.sinceSCN.set(sinceSCN);
		EventProducerThread thread = new EventProducerThread();
		thread.start();
	}

	/**
	 * Interface method implementation.
	 * @see com.linkedin.databus2.producers.EventProducer#getSCN()
	 */
	public long getSCN() {
		return this.sinceSCN.get();
	}

	/**
	 * Interface method implementation. Returns false always
	 * @see com.linkedin.databus2.producers.EventProducer#isPaused()
	 */
	public boolean isPaused() {
		return false;
	}

	/**
	 * Interface method implementation. Returns true always
	 * @see com.linkedin.databus2.producers.EventProducer#isRunning()
	 */
	public boolean isRunning() {
		return true;
	}

	/** No Op methods*/
	public void pause() {}
	
	public void shutdown() 
	{
		LOGGER.info("Shutdown has been requested. PersonEventProducer shutttng down");
		shutdownRequested.set(true);
		super.shutdown();
		LOGGER.info("PersonEventProducer shutdown completed");
	}
	
	public void unpause() {}
	public void waitForShutdown() throws InterruptedException,IllegalStateException {}
	public void waitForShutdown(long time) throws InterruptedException,IllegalStateException {}
	
	/** Setter/Getter methods*/
	public void setNumberOfEventsPerRun(int numberOfEventsPerRun) {
		this.numberOfEventsPerRun = numberOfEventsPerRun;
	}
	
	/** Thread that creates a specified number of Person instances from in-memory data*/
	private class EventProducerThread extends Thread {
		public void run() {
			while (!shutdownRequested.get()) {
                int sleep = (int) (Math.random()*5000);
				getEventBuffer().startEvents();
				long endValue = sinceSCN.longValue() + (int) (Math.random()*numberOfEventsPerRun);
				for (long i = sinceSCN.longValue(); i < endValue; i++) {
					Person person = new Person(i, "Aesop " + i, "Mr. " + i, i,"false",new LinkedList<FieldChange>());
					byte[] serializedEvent = serializeEvent(person);
					byte[] schemaId=SchemaHelper.getSchemaId(person.getSchema().toString());
					DbusEventKey eventKey = new DbusEventKey(i);
					DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,i,
							(short)physicalSourceStaticConfig.getId(),(short)physicalSourceStaticConfig.getId(),
							System.nanoTime(),(short)physicalSourceStaticConfig.getSources()[0].getId(), // here we use the Logical Source Id
							schemaId,serializedEvent, false, true);
					getEventBuffer().appendEvent(eventKey, eventInfo, dbusEventsStatisticsCollector);
					sinceSCN.getAndIncrement();
					LOGGER.info("Added an event : " + "Aesop Mr. " + i);
				}
				getEventBuffer().endEvents(sinceSCN.longValue(),dbusEventsStatisticsCollector);
				try {
					maxScnReaderWriter.saveMaxScn(sinceSCN.longValue() + numberOfEventsPerRun);
				} catch (DatabusException e) {
					LOGGER.error("Error persisting Max SCN : " + e.getMessage(), e);
				}
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
