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
package org.aesop.runtime.spring;

import org.aesop.runtime.RuntimeFrameworkConstants;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.Resource;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.spi.event.PlatformEventProducer;
import org.trpr.platform.model.event.PlatformEvent;
import org.trpr.platform.runtime.spi.bootstrapext.BootstrapExtension;
import org.trpr.platform.runtime.spi.component.ComponentContainer;

import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * The <code>RuntimeComponentContainer</code> class is a ComponentContainer implementation as defined by Trooper {@link "https://github.com/regunathb/Trooper"} that 
 * starts up one or more Databus {@link ServerContainer} instances defined in {@link #getRuntimeConfigFileName()} This container also loads all common
 * runtime related Spring beans contained in {@link RuntimeFrameworkConstants#COMMON_RUNTIME_CONFIG} and ensures that these common beans are available to the runtime 
 * by specifying the common beans context as the parent for each runtime app context created by this container.
 * 
 * @see org.trpr.platform.runtime.spi.component.ComponentContainer
 * @author Regunath B
 * @version 1.0, 03 Jan 2014
 */
public abstract class RuntimeComponentContainer implements ComponentContainer {

	/**
	 * The default Event producer bean name
	 */
	private static final String DEFAULT_EVENT_PRODUCER = "platformEventProducer";

	/** The common runtime beans context */
	private static AbstractApplicationContext commonRuntimeBeansContext;

	/** Local reference for all BootstrapExtensionS loaded by the Container and set on this ComponentContainer*/
	private BootstrapExtension[] loadedBootstrapExtensions;

	/** The Thread's context class loader that is used in on the fly loading of ServerContainer definitions */
	private ClassLoader tccl;
	
	/**
	 * Interface method implementation. Stores local references to the specified BootstrapExtension instances.
	 * @see org.trpr.platform.runtime.spi.component.ComponentContainer#setLoadedBootstrapExtensions(org.trpr.platform.runtime.spi.bootstrapext.BootstrapExtension[])
	 */
	public void setLoadedBootstrapExtensions(BootstrapExtension...bootstrapExtensions) {
		this.loadedBootstrapExtensions = bootstrapExtensions;
	}

	/**
	 * Interface method implementation. Returns the fully qualified class name of this class
	 * @see org.trpr.platform.runtime.spi.component.ComponentContainer#getName()
	 */
	public String getName() {
		return this.getClass().getName();
	}
	
	public void init() throws PlatformException {
	}

	public void destroy() throws PlatformException {
	}

	public void loadComponent(Resource resource) {
	}

	/**
	 * Interface method implementation. Publishes the specified event using the
	 * {@link #publishEvent(PlatformEvent)} method
	 * 
	 * @see org.trpr.platform.runtime.spi.component.ComponentContainer#publishBootstrapEvent(org.trpr.platform.model.event.PlatformEvent)
	 */
	public void publishBootstrapEvent(PlatformEvent bootstrapEvent) {
		this.publishEvent(bootstrapEvent);
	}

	/**
	 * Interface method implementation. Publishes the specified event to using a named bean DEFAULT_EVENT_PRODUCER looked up from the common runtime
	 * context (i.e. {@link RuntimeFrameworkConstants#COMMON_RUNTIME_CONFIG}). Note that typically no consumers are registered when running this container
	 * 
	 * @see org.trpr.platform.core.spi.event.PlatformEventProducer#publishEvent(org.trpr.platform.model.event.PlatformEvent)
	 */
	public void publishEvent(PlatformEvent event) {
		PlatformEventProducer publisher = (PlatformEventProducer) RuntimeComponentContainer.commonRuntimeBeansContext.getBean(DEFAULT_EVENT_PRODUCER);
		publisher.publishEvent(event);
	}

	/**
	 * Returns the config file name that defines bean instances of type {@link ServerContainer}
	 * @return Spring beans file name containing bean deifnitions of type {@link ServerContainer}
	 */
	protected abstract String getRuntimeConfigFileName();

}
