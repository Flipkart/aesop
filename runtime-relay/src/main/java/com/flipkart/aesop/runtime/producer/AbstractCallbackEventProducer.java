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

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.linkedin.databus.core.DatabusComponentStatus;
import com.linkedin.databus.core.DbusEventInfo;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.schemas.utils.SchemaHelper;


/**
 * <code>AbstractCallbackEventProducer</code> is a sub-type of the {@link AbstractEventProducer} that provides a callback method for producing change events. This
 * callback method {@link #readEventsFromAllSources(long)} is invoked in a EventProducer thread managed by this class. This implementation is a port of the Databus
 * {@link com.linkedin.databus2.producers.AbstractEventProducer} with some modifications.
 *
 * @author Regunath B
 * @version 1.0, 18 March 2014
 */
public abstract class AbstractCallbackEventProducer<S extends GenericRecord> extends AbstractEventProducer implements InitializingBean {

	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(AbstractCallbackEventProducer.class);

	/** Possible states for the Event producer thread*/
	private static final int ACTIVE = 0;
	private static final int PAUSED = 1;
	private static final int EXIT = 2;
	
	/** The event generation thread and various related member variables */
	protected EventProducerThread eventThread;
	protected volatile int eventThreadState = ACTIVE;
	
	/** The Databus component status*/
	protected DatabusComponentStatus status;
	
	/**
	 * Interface method implementation. Checks for mandatory dependencies and creates the DatabusComponentStatus
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.physicalSourceConfig,"'physicalSourceConfig' cannot be null. This Event producer will not be initialized");
		this.status = new DatabusComponentStatus(name + ".callbackEventProducer", physicalSourceConfig.getRetries().build());
	}
	
	/**
	 * Interface method implementation. Starts up the EventProducerThread
	 * @see com.linkedin.databus2.producers.EventProducer#start(long)
	 */
	public void start (long sinceSCN) {
		eventThreadState = ACTIVE;
		this.sinceSCN.set(sinceSCN);
		this.eventThread = new EventProducerThread(name);
		this.eventThread.setDaemon(true);
		this.eventThread.start();
		LOGGER.info("Started callback event producer : {} from SCN ; {}",this.getName(), sinceSCN);
	}
	
	/**
	 * Interface method implementation. Stops the EventProducerThread
	 * @see com.linkedin.databus2.producers.EventProducer#shutdown()
	 */
	public void shutdown() {
		synchronized(this) {
			this.eventThreadState = EXIT;
			notifyAll();
			if(eventThread != null) {
				eventThread.interrupt(); // in case it is sleeping 
			}
		}
		//shutdown() call on super - Not required here as roll back of events is being done in the EventProducerThread
	}

	/**
	 * Interface method implementation. Returns if pause has been requested
	 * @see com.linkedin.databus2.producers.EventProducer#isPaused()
	 */
	public boolean isPaused() {
		return this.eventThreadState == PAUSED;
	}

	/**
	 * Interface method implementation. Returns true if shutdown or pause has not been requested
	 * @see com.linkedin.databus2.producers.EventProducer#isRunning()
	 */
	public boolean isRunning() {
		return this.eventThread != null && this.eventThreadState != PAUSED && this.eventThreadState != EXIT;
	}

	/**
	 * Interface method implementation. Pauses the event producer thread 
	 * @see com.linkedin.databus2.producers.EventProducer#pause()
	 */
	public void pause() {
		synchronized(this) {
			this.eventThreadState = PAUSED;
			notifyAll();
		}
	}
	
	/**
	 * Interface method implementation. Resumes the paused event producer thread
	 * @see com.linkedin.databus2.producers.EventProducer#unpause()
	 */
	public void unpause() {
		synchronized(this) {
			this.eventThreadState = ACTIVE;
			notifyAll();
		}		
	}
	
	/**
	 * Interface method implementation. Waits for the event producer thread to shutdown
	 * @see com.linkedin.databus2.producers.EventProducer#waitForShutdown()
	 */
	public void waitForShutdown() throws InterruptedException,IllegalStateException {
		while (eventThread != null && eventThread.isAlive()) {
			eventThread.join();
		}
	}
	
	/**
	 * Interface method implementation.  Waits for the event producer thread to shutdown within the specified timeout
	 * @see com.linkedin.databus2.producers.EventProducer#waitForShutdown(long)
	 */
	public void waitForShutdown(long time) throws InterruptedException,IllegalStateException {
		if (eventThread != null && eventThread.isAlive()) {
			eventThread.join(time);
		}		
		if (eventThread != null && eventThread.isAlive()) {
			throw new IllegalStateException("Shutdown not successful on event producer thread for :" + name + " after timeout of : " + time);
		}		
	}
	
	/**
	 * Callback method to be implemented by sub-types to produce change events
	 * @return the maximum end of window SCN read across sources
	 * @param sinceSCN the SCN reference for producing events
	 * @throws EventCreationException in case of errors in event creation
	 */
	protected abstract ReadEventCycleSummary<S> readEventsFromAllSources(long sinceSCN) throws EventCreationException;
	
	/**
	 * Returns a key for the change event created
	 * @param changeEvent the change event
	 * @return key for the change event
	 */
	protected abstract Object getEventKey(S changeEvent);
	
	/**
	 * Returns a sequence number for the change event
	 * @param changeEvent the change event
	 * @return sequence number for the change event
	 */
	protected abstract Long getSequenceId(S changeEvent);
	
	private class EventProducerThread extends Thread {
		/** Constructor for this class*/
	    public EventProducerThread(String producerName) {
	      super("EventProducerThread_" + producerName);
	    }
	    /**
	     * Thread run method implementation. Calls the callback method {@link AbstractCallbackEventProducer#readEventsFromAllSources(long)} to produce events
	     * and sleeps between runs.
	     * @see java.lang.Thread#run()
	     */
	    public void run() {
	    	while(true) {
	    		synchronized(this) {
	    			switch (eventThreadState) {
	    			case PAUSED:
	    				LOGGER.info("EventProducerThread for {} is pausing because a pause was requested.", getName());
	    	            try {
	    	            	wait();
	    	            } catch(InterruptedException ex) {
	    	            	LOGGER.info("Ignoring thread interrupt on EventProducerThread for {}", getName());
	    	            }	    				
	    				break;
	    			case EXIT:
	    				LOGGER.info("EventProducerThread for {} is stopping because a shutdown was requested.", getName());
	    	            eventBuffer.rollbackEvents();
	    	            return;
	    			case ACTIVE:
	    				try {
	    					ReadEventCycleSummary<S> readEventCycleSummary = readEventsFromAllSources(sinceSCN.get());
	    					if (readEventCycleSummary.getChangeEvents().size() > 0) {
		    					eventBuffer.startEvents();
		    					for (S changeEvent : readEventCycleSummary.getChangeEvents()) {
		    						byte[] schemaId=SchemaHelper.getSchemaId(changeEvent.getSchema().toString());
		    						byte[] serializedEvent = serializeEvent(changeEvent);
		    						DbusEventKey eventKey = new DbusEventKey(getEventKey(changeEvent));
		    						DbusEventInfo eventInfo = new DbusEventInfo(DbusOpcode.UPSERT,getSequenceId(changeEvent),
		    								(short)physicalSourceStaticConfig.getId(),(short)physicalSourceStaticConfig.getId(),
		    								System.nanoTime(),(short)physicalSourceStaticConfig.getSources()[0].getId(), // here we use the Logical Source Id
		    								schemaId,serializedEvent, false, true);
		    						eventBuffer.appendEvent(eventKey, eventInfo, dbusEventsStatisticsCollector);	    						
		    					}
								long endOfWindowScn = readEventCycleSummary.getSinceSCN();
								long newSinceSCN = Math.max(endOfWindowScn, sinceSCN.get());
								sinceSCN.set(newSinceSCN);
					            eventBuffer.endEvents(sinceSCN.get() , dbusEventsStatisticsCollector);
								maxScnReaderWriter.saveMaxScn(sinceSCN.get());
								LOGGER.info("Added {} change events to event buffer for : {} . New SCN is : " + newSinceSCN, readEventCycleSummary.getChangeEvents().size(), getName());
	    					}
							if (status.getRetriesNum() > 0) {
								status.resume();
							}
							status.getRetriesCounter().reset();
							status.getRetriesCounter().sleep(); // sleep until the next cycle
						} catch (Exception e) {
							LOGGER.error("Event creation exception occurred reading events from : {}. Error is : " + e.getMessage(),getName(),e);
							status.retryOnError(getName() + " error: " + e.getMessage());
						}
	    				break;
	    			}
	    		}
	    	}
	    }
	}
}
