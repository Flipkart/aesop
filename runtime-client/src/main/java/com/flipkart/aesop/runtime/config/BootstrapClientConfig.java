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
 * <code>BootstrapClientConfig</code> holds Databus configuration properties for a Bootstrap Client instance. This config treats the
 * properties as opaque and is intended for use as a holder of the information.
 *
 * @author Regunath B, Jagadeesh Huliyar
 * @version 1.0, 13 Jan 2014
 */

public class BootstrapClientConfig implements InitializingBean {

	/** The identifier for the Bootstrap that the Bootstrap Client is connecting to*/
	private String bootstrapId;
	
	/** The Bootstrap host DNS name*/
	private String bootstrapHost;
	
	/** The Bootstrap port number*/
	private Integer bootstrapPort;
	
	/** List of Logical Source names in the Bootstrap that the Bootstrap Client will consume change events from */
	private List<String> bootstrapLogicalSourceNames;
		
	/**
	 * Interface method implementation. Ensures that all mandatory properties are set
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.bootstrapId,"'bootstrapId' cannot be null. The Bootstrap Client will not be initialized");		
		Assert.notNull(this.bootstrapHost,"'bootstrapHost' cannot be null. The Bootstrap Client will not be initialized");		
		Assert.notNull(this.bootstrapPort,"'bootstrapPort' cannot be null. The Bootstrap Client will not be initialized");		
		Assert.notNull(this.bootstrapLogicalSourceNames,"'bootstrapLogicalSourceNames' cannot be null. The Bootstrap Client will not be initialized");		
	}

	/** Getter/Setter properties*/		
	public String getBootstrapId() {
		return bootstrapId;
	}
	public void setBootstrapId(String bootstrapId) {
		this.bootstrapId = bootstrapId;
	}
	public String getBootstrapHost() {
		return bootstrapHost;
	}
	public void setBootstrapHost(String bootstrapHost) {
		this.bootstrapHost = bootstrapHost;
	}
	public Integer getBootstrapPort() {
		return bootstrapPort;
	}
	public void setBootstrapPort(Integer bootstrapPort) {
		this.bootstrapPort = bootstrapPort;
	}
	public List<String> getBootstrapLogicalSourceNames() {
		return bootstrapLogicalSourceNames;
	}
	public void setBootstrapLogicalSourceNames(List<String> bootstrapLogicalSourceName) {
		this.bootstrapLogicalSourceNames = bootstrapLogicalSourceName;
	}
}
