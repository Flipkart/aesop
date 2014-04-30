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

package com.flipkart.aesop.runtime.config;

import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.aesop.runtime.client.DefaultClient;
import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;

/**
 * <code>ConsumerRegistration</code> holds information for registering a Databus {@link AbstractDatabusCombinedConsumer} with the
 * {@link DefaultClient} against a Relay Logical Source name
 *
 * @author Regunath B, Jagadeesh Huliyar
 * @version 1.0, 24 Jan 2014
 */
public class ConsumerRegistration implements InitializingBean {

	/** The AbstractDatabusCombinedConsumer to be registered*/
	private AbstractDatabusCombinedConsumer eventConsumer;
	
	/** The Logical Sources */
	private List<String> logicalSources;
	
	private boolean readFromBootstrap;

	/**
	 * Interface method implementation. Ensures that a AbstractDatabusCombinedConsumer and Relay Logical Source name is set
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.eventConsumer,"'eventConsumer' cannot be null. An AbstractDatabusCombinedConsumer must be specified");
		Assert.notNull(this.logicalSources,"'logicalSources' cannot be null. Logical sources must be specified");
	}

	/** Getter/Setter methods*/	
	public AbstractDatabusCombinedConsumer getEventConsumer() {
		return eventConsumer;
	}
	public void setEventConsumer(AbstractDatabusCombinedConsumer eventConsumer) {
		this.eventConsumer = eventConsumer;
	}

	public List<String> getLogicalSources()
	{
		return logicalSources;
	}

	public void setLogicalSources(List<String> logicalSources)
	{
		this.logicalSources = logicalSources;
	}

	public boolean isReadFromBootstrap()
	{
		return readFromBootstrap;
	}

	public void setReadFromBootstrap(boolean readFromBootstrap)
	{
		this.readFromBootstrap = readFromBootstrap;
	}
}
