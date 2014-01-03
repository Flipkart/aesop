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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

import org.aesop.runtime.RuntimeFrameworkConstants;
import org.aesop.runtime.spring.registry.ServerContainerConfigInfo;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.FileSystemResource;
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

	/** The list of ServerContainerConfigInfo holding all runtime instances loaded by this container */
	private List<ServerContainerConfigInfo> runtimeConfigInfoList = new LinkedList<ServerContainerConfigInfo>();
	
	/** Local reference for all BootstrapExtensionS loaded by the Container and set on this ComponentContainer*/
	private BootstrapExtension[] loadedBootstrapExtensions;

	/** The Thread's context class loader that is used in on the fly loading of ServerContainer definitions */
	private ClassLoader tccl;
	
	/**
	 * Returns the common Runtime Spring beans application context that is intended as parent of all Runtime application contexts 
	 * WARN : this method can return null if this ComponentContainer is not suitably initialized via a call to {@link #init()}
	 * @return null or the common runtime AbstractApplicationContext
	 */
	public static AbstractApplicationContext getCommonRuntimeBeansContext() {
		return RuntimeComponentContainer.commonRuntimeBeansContext;
	}

	/**
	 * Interface method implementation. Returns the fully qualified class name of this class
	 * @see org.trpr.platform.runtime.spi.component.ComponentContainer#getName()
	 */
	public String getName() {
		return this.getClass().getName();
	}
	
	/**
	 * Interface method implementation. Stores local references to the specified BootstrapExtension instances.
	 * @see org.trpr.platform.runtime.spi.component.ComponentContainer#setLoadedBootstrapExtensions(org.trpr.platform.runtime.spi.bootstrapext.BootstrapExtension[])
	 */
	public void setLoadedBootstrapExtensions(BootstrapExtension...bootstrapExtensions) {
		this.loadedBootstrapExtensions = bootstrapExtensions;
	}

	public void init() throws PlatformException {
	}

	public void destroy() throws PlatformException {
	}

	/**
	 * Interface method implementation. Loads/Reloads runtime(s) defined in the specified {@link FileSystemResource} 
	 * @see org.trpr.platform.runtime.spi.component.ComponentContainer#loadComponent(org.springframework.core.io.Resource)
	 */
	public void loadComponent(Resource resource) {
		if (!FileSystemResource.class.isAssignableFrom(resource.getClass()) || 
				!resource.getFilename().equalsIgnoreCase(this.getRuntimeConfigFileName())) {
			throw new UnsupportedOperationException("Runtimes can be loaded only from files by name : " + 
					this.getRuntimeConfigFileName() + ". Specified resource is : " + resource.toString());
		}
		loadRuntimeContext(new ServerContainerConfigInfo(((FileSystemResource)resource).getFile()));
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
	public abstract String getRuntimeConfigFileName();

	/**
	 * Loads the runtime context from path specified in the ServerContainerConfigInfo. Looks for file by name {@link RuntimeComponentContainer#getRuntimeConfigFileName()}.
	 * @param serverContainerConfigInfo containing absolute path to the runtime's configuration location i.e. folder
	 */
	private void loadRuntimeContext(ServerContainerConfigInfo serverContainerConfigInfo) {
		// check if a context exists already for this config path 
		for (ServerContainerConfigInfo loadedRuntimeConfigInfo : this.runtimeConfigInfoList) {
			if (loadedRuntimeConfigInfo.equals(serverContainerConfigInfo)) {
				serverContainerConfigInfo = loadedRuntimeConfigInfo;
				break;
			}
		}
		if (serverContainerConfigInfo.getRuntimeContext() != null) {
			// close the context and remove from list
			serverContainerConfigInfo.getRuntimeContext().close();
			this.runtimeConfigInfoList.remove(serverContainerConfigInfo);
		}
		ClassLoader runtimeCL = this.tccl;
		// check to see if the runtime and dependent binaries are deployed outside of the runtime class path. If yes, include them using a custom URL classloader.
		File customLibPath = new File (serverContainerConfigInfo.getXmlConfigFile().getParentFile(), ServerContainerConfigInfo.BINARIES_PATH);
		if (customLibPath.exists() && customLibPath.isDirectory()) {
			try {
				File[] libFiles = customLibPath.listFiles();
				URL[] libURLs = new URL[libFiles.length];
				for (int i=0; i < libFiles.length; i++) {
					libURLs[i] = new URL(ServerContainerConfigInfo.FILE_PREFIX + libFiles[i].getAbsolutePath());
				}
				runtimeCL = new URLClassLoader(libURLs, this.tccl);
			} catch (MalformedURLException e) {
				throw new PlatformException(e);
			}
		} 
		// now load the runtime context and add it into the serverContainerConfigInfo list
		serverContainerConfigInfo.loadRuntimeContext(runtimeCL,RuntimeComponentContainer.getCommonRuntimeBeansContext());
		this.runtimeConfigInfoList.add(serverContainerConfigInfo);		
	}
	
}
