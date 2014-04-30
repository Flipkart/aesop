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

/**
 * <code>RelayClientConfig</code> holds Databus configuration properties for a Relay Client instance. This config treats the
 * properties as opaque and is intended for use as a holder of the information.
 *
 * @author Regunath B
 * @version 1.0, 13 Jan 2014
 */

public class RelayClientConfig implements InitializingBean {

	/** The identifier for the Relay that the Relay Client is connecting to*/
	private String relayId;
	
	/** The Relay host DNS name*/
	private String relayHost;
	
	/** The Relay port number*/
	private Integer relayPort;
	
	/** List of Logical Source names in the Relay that the Relay Client will consume change events from */
	private List<String> relayLogicalSourceNames;
	
	/**
	 * Interface method implementation. Ensures that all mandatory properties are set
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.relayId,"'relayId' cannot be null. The Relay Client will not be initialized");		
		Assert.notNull(this.relayHost,"'relayHost' cannot be null. The Relay Client will not be initialized");		
		Assert.notNull(this.relayPort,"'relayPort' cannot be null. The Relay Client will not be initialized");		
		Assert.notNull(this.relayLogicalSourceNames,"'relayLogicalSourceName' cannot be null. The Relay Client will not be initialized");		
	}
	
	/** Getter/Setter properties*/		
	public String getRelayId() {
		return relayId;
	}	
	public String getRelayHost() {
		return relayHost;
	}
	public void setRelayHost(String relayHost) {
		this.relayHost = relayHost;
	}
	public Integer getRelayPort() {
		return relayPort;
	}
	public void setRelayPort(Integer relayPort) {
		this.relayPort = relayPort;
	}
	public void setRelayId(String relayId) {
		this.relayId = relayId;
	}
	public List<String> getRelayLogicalSourceNames() {
		return relayLogicalSourceNames;
	}
	public void setRelayLogicalSourceNames(List<String> relayLogicalSourceNames) {
		this.relayLogicalSourceNames = relayLogicalSourceNames;
	}	
	
}
