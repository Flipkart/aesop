/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.spring;

/**
 * The <code>BlockingBootstrapRuntimeComponentContainer</code> class is a concrete subtype of the
 * {@link RuntimeComponentContainer} used for managing blocking databus bootstrap instances that serve change event
 * snapshots.
 * @see org.trpr.platform.runtime.spi.component.ComponentContainer
 * @author nrbafna
 */
public class BlockingBootstrapRuntimeComponentContainer extends RuntimeComponentContainer
{
	/**
	 * The file name containing Http Client beans
	 */
	public static final String BOOTSTRAP_CONFIG_FILE = "spring-blocking-bootstrap-config.xml";

	/**
	 * The runtime module name
	 */
	public static final String RUNTIME_MODULE_NAME = "runtime-blocking-bootstrap";

	/**
	 * Abstract method implementation.
	 * @see com.flipkart.aesop.runtime.spring.RuntimeComponentContainer#getRuntimeConfigFileName()
	 */
	public String getRuntimeConfigFileName()
	{
		return BlockingBootstrapRuntimeComponentContainer.BOOTSTRAP_CONFIG_FILE;
	}

	/**
	 * Abstract method implementation.
	 * @see com.flipkart.aesop.runtime.spring.RuntimeComponentContainer#getRuntimeModuleName()
	 */
	public String getRuntimeModuleName()
	{
		return BlockingBootstrapRuntimeComponentContainer.RUNTIME_MODULE_NAME;
	}

}
