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
package org.aesop.relay.sample;

import org.aesop.events.sample.person.Person;
import org.aesop.runtime.producer.AbstractEventProducer;

/**
 * <code>PersonEventProducer</code> is a sub-type of {@link AbstractEventProducer}} that creates a specified number 
 * of change events of type {@link Person} using in-memory data. The events are created in
 * a separate thread and appended to the Databus event buffer instance.
 *
 * @author Regunath B
 * @version 1.0, 17 Jan 2014
 */
public class PersonEventProducer extends AbstractEventProducer {

	/** The default number of events to produce in a single run*/
	private static final int NUM_EVENTS = 100;
	
	/** Member variables related to events production an handling*/
	private int numberOfEventsPerRun = NUM_EVENTS;
	
	public void setNumberOfEventsPerRun(int numberOfEventsPerRun) {
		this.numberOfEventsPerRun = numberOfEventsPerRun;
	}
		
}
