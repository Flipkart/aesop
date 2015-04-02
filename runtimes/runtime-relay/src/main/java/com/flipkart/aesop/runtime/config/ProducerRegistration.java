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

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.flipkart.aesop.runtime.relay.DefaultRelay;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;

import java.util.Properties;

/**
 * <code>ProducerRegistration</code> holds information for registering a Databus {@link EventProducer} with the
 * {@link DefaultRelay} against a {@link PhysicalSourceConfig}
 *
 * @author Regunath B
 * @version 1.0, 15 Jan 2014
 */
public class ProducerRegistration implements InitializingBean {

	/** The EventProducer to be registered*/
	private EventProducer eventProducer;
	
	/** The physical databus source configuration*/
	private PhysicalSourceConfig physicalSourceConfig;

    /* Load AdditionalProperties if Its Present */
    private Properties properties;
	
	/**
	 * Interface method implementation. Ensures that a EventProducer and PhysicalSourceConfig is set
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.eventProducer,"'eventProducer' cannot be null. An EventProducer must be specified");
		Assert.notNull(this.physicalSourceConfig,"'physicalSourceConfig' cannot be null. A PhysicalSourceConfig must be specified");
	}

	/** Getter/Setter methods*/
	public EventProducer getEventProducer() {
		return this.eventProducer;
	}
	public void setEventProducer(EventProducer eventProducer) {
		this.eventProducer = eventProducer;
	}
	public PhysicalSourceConfig getPhysicalSourceConfig() {
		return this.physicalSourceConfig;
	}
	public void setPhysicalSourceConfig(PhysicalSourceConfig physicalSourceConfig) {
		this.physicalSourceConfig = physicalSourceConfig;
	}
	public Properties getProperties() {
		return properties;
	}
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}
