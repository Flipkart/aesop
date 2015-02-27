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
package com.flipkart.aesop.runtime.spring;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.event.PlatformEventProducer;
import org.trpr.platform.core.spi.logging.Logger;
import org.trpr.platform.model.event.PlatformEvent;
import org.trpr.platform.runtime.common.RuntimeConstants;
import org.trpr.platform.runtime.common.RuntimeVariables;
import org.trpr.platform.runtime.impl.bootstrapext.spring.ApplicationContextFactory;
import org.trpr.platform.runtime.impl.config.FileLocator;
import org.trpr.platform.runtime.spi.bootstrapext.BootstrapExtension;
import org.trpr.platform.runtime.spi.component.ComponentContainer;

import com.flipkart.aesop.runtime.RuntimeFrameworkConstants;
import com.flipkart.aesop.runtime.impl.admin.RuntimeConfigServiceImpl;
import com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService;
import com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry;
import com.flipkart.aesop.runtime.spring.registry.ServerContainerConfigInfo;
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
	 * The Runtime variable that holds the sub-type specific module name 
	 */
	public static final String RUNTIME_MODULE_VAR = "com.flipkart.aesop.runtime.module.name";

	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(RuntimeComponentContainer.class);
	
	/**
	 * The default Event producer bean name
	 */
	private static final String DEFAULT_EVENT_PRODUCER = "platformEventProducer";
	
	/** The bean names of the runtime framework classes initialized by this container */
	private static final String CONFIG_SERVICE_BEAN = "configService";
	
	/** The common runtime beans context */
	private static AbstractApplicationContext commonRuntimeBeansContext;

	/** The list of ServerContainerConfigInfo holding all runtime instances loaded by this container */
	private List<ServerContainerConfigInfo> runtimeConfigInfoList = new LinkedList<ServerContainerConfigInfo>();
	
	/** Local reference for all BootstrapExtensionS loaded by the Container and set on this ComponentContainer*/
	private BootstrapExtension[] loadedBootstrapExtensions;

	/** The Thread's context class loader that is used in on the fly loading of ServerContainer definitions */
	private ClassLoader tccl;
	
    /** The list of registered registries */
    private List<AbstractRuntimeRegistry> registries = new ArrayList<AbstractRuntimeRegistry>();
	
	/** The configService instance */
	private RuntimeConfigService configService;
	
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

	/**
	 * Interface method implementation. Locates and loads all configured runtime instances.
	 * @see ComponentContainer#init()
	 */
	public void init() throws PlatformException {
		// store the thread's context class loader for later use in on the fly loading of runtime app contexts
		this.tccl = Thread.currentThread().getContextClassLoader();

		// populate the instance specific module name into runtime variables for use by other classes loaded by this component container
		RuntimeVariables.getInstance().setVariable(RuntimeComponentContainer.RUNTIME_MODULE_VAR, this.getRuntimeModuleName());
		
		// The common runtime beans context is loaded first using the Platform common beans context as parent
		// load this from classpath as it is packaged with the binaries
		ApplicationContextFactory defaultCtxFactory = null;
		for (BootstrapExtension be : this.loadedBootstrapExtensions) {
			if (ApplicationContextFactory.class.isAssignableFrom(be.getClass())) {
				defaultCtxFactory = (ApplicationContextFactory)be;
				break;
			}
		}

		RuntimeComponentContainer.commonRuntimeBeansContext = new ClassPathXmlApplicationContext(new String[]{RuntimeFrameworkConstants.COMMON_RUNTIME_CONFIG},defaultCtxFactory.getCommonBeansContext());
		// add the common runtime beans independently to the list of runtime contexts 
		this.runtimeConfigInfoList.add(new ServerContainerConfigInfo(new File(RuntimeFrameworkConstants.COMMON_RUNTIME_CONFIG), null, RuntimeComponentContainer.commonRuntimeBeansContext));
		
		// Get the Config Service Bean
		this.configService = (RuntimeConfigServiceImpl)RuntimeComponentContainer.commonRuntimeBeansContext.getBean(RuntimeComponentContainer.CONFIG_SERVICE_BEAN);
        ((RuntimeConfigServiceImpl)this.configService).setComponentContainer(this);
		
		// Load additional if runtime nature is "server". This context is the new common beans context
		if (RuntimeVariables.getRuntimeNature().equalsIgnoreCase(RuntimeConstants.SERVER)) {
			RuntimeComponentContainer.commonRuntimeBeansContext = new ClassPathXmlApplicationContext(
                new String[]{RuntimeFrameworkConstants.COMMON_RUNTIME_SERVER_NATURE_CONFIG},
                RuntimeComponentContainer.getCommonRuntimeBeansContext()
            );
			// now add the common server nature runtime beans to the contexts list
			this.runtimeConfigInfoList.add(new ServerContainerConfigInfo(new File(RuntimeFrameworkConstants.COMMON_RUNTIME_SERVER_NATURE_CONFIG),
                    null, RuntimeComponentContainer.getCommonRuntimeBeansContext()
                )
            );
		}
		
	    // locate and load the individual runtime XML files using the common runtime beans context as parent
        File[] runtimeBeansFiles = FileLocator.findFiles(this.getRuntimeConfigFileName());
        for (File runtimeBeansFile : runtimeBeansFiles) {
        	ServerContainerConfigInfo runtimeConfigInfo = new ServerContainerConfigInfo(runtimeBeansFile);
            // load the runtime's app context
            this.loadRuntimeContext(runtimeConfigInfo);
            LOGGER.info("Loaded: " + runtimeBeansFile);
        }
		
        // load all registries
        for (ServerContainerConfigInfo serverContainerConfigInfo : this.runtimeConfigInfoList) {
            // runtime registries
            String[] registryBeans = serverContainerConfigInfo.getRuntimeContext().getBeanNamesForType(AbstractRuntimeRegistry.class);
            for (String registryBean:registryBeans) {
            	AbstractRuntimeRegistry registry = (AbstractRuntimeRegistry) serverContainerConfigInfo.getRuntimeContext().getBean(registryBean);
                LOGGER.info("Found runtime registry: " + registry.getClass().getName());
                // init the Registry
                try {
                	AbstractRuntimeRegistry.InitedRuntimeInfo[] initedRuntimeInfos = registry.init(this.runtimeConfigInfoList);
                    LOGGER.info("Initialized runtime registry: " + registry.getClass().getName());
        			//Add the file path of each inited runtime to RuntimeConfigService (for configuration console)
                    for (AbstractRuntimeRegistry.InitedRuntimeInfo initedRuntimeInfo: initedRuntimeInfos) {
                    	this.configService.addRuntimeConfigPath(initedRuntimeInfo.getRuntimeConfigInfo().getXmlConfigFile(), initedRuntimeInfo.getInitedRuntime());
                    }
                } catch (Exception e) {
                    LOGGER.error("Error initializing registry: " + registry.getClass().getName());
                    throw new PlatformException("Error initializing registry: " + registry.getClass().getName(), e);
                }
                // add registry to config
                this.configService.addRuntimeRegistry(registry);
                // add registry to local list
                this.registries.add(registry);
            }
        }
        
	}

	/**
	 * Interface method implementation. Destroys the Spring application context containing loaded runtime definitions.
	 * @see ComponentContainer#destroy()
	 */
	public void destroy() throws PlatformException {
		// shutdown all runtime instances
        for (AbstractRuntimeRegistry registry:registries) {
            try {
                registry.shutdown();
            } catch (Exception e) {
                LOGGER.warn("Error shutting down registry: " + registry.getClass().getName());
            }
        }
        // finally close the context
		for (ServerContainerConfigInfo runtimeConfigInfo : this.runtimeConfigInfoList) {
			runtimeConfigInfo.getRuntimeContext().close();
		}
		this.runtimeConfigInfoList = null;
	}

	/**
	 * Reloads and re-initalizes the specified runtime. The new definition is loaded from the specified Resource location
	 * @param runtime the ServerContainer to be de-registered
	 * @param resource the location to load the new definition of the runtime from
	 */
	public void reloadRuntime(ServerContainer runtime, Resource resource) {
		AbstractRuntimeRegistry registry = this.getRegistry(runtime.getComponentAdmin().getComponentName());
		registry.unregisterRuntime(runtime);
		LOGGER.debug("Unregistered ServerContainer: "+ runtime.getComponentAdmin().getComponentName());
		this.loadComponent(resource);
		// now add the newly loaded runtime to its registry
		for (ServerContainerConfigInfo runtimeConfigInfo : this.runtimeConfigInfoList) {
			if (runtimeConfigInfo.getXmlConfigFile().getAbsolutePath().equalsIgnoreCase(((FileSystemResource)resource).getFile().getAbsolutePath())) {
				List<ServerContainerConfigInfo> reloadRuntimeConfigInfoList = new LinkedList<ServerContainerConfigInfo>();
				reloadRuntimeConfigInfoList.add(runtimeConfigInfo);
				try {
					registry.init(reloadRuntimeConfigInfoList);
				}catch (Exception e) {
		            LOGGER.error("Error updating registry : " +  registry.getClass().getName() + " for runtime : " + runtime.getComponentAdmin().getComponentName(), e);
		            throw new PlatformException("Error updating registry : " +  registry.getClass().getName() + " for runtime : " + runtime.getComponentAdmin().getComponentName(), e);
		        }
				return;
			}
		}
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
	 * Returns the AbstractRuntimeRegistry in which a ServerContainer identified by the specified name has been registered
	 * @param runtimeName the ServerContainer name
	 * @return AbstractRuntimeRegistry where the runtime is registered
	 * @throws UnsupportedOperationException if a registry is not found
	 */
	public AbstractRuntimeRegistry getRegistry (String runtimeName) {
		for (AbstractRuntimeRegistry registry : this.registries) {
			if (registry.getRuntime(runtimeName) != null) {
				return registry;
			}
		}
		throw new UnsupportedOperationException("No known registries exist for ServerContainer by name : " + runtimeName);
	}
	
	/**
	 * Returns the config file name that defines bean instances of type {@link ServerContainer}
	 * @return Spring beans file name containing bean definitions of type {@link ServerContainer}
	 */
	public abstract String getRuntimeConfigFileName();
	
	/** 
	 * Returns the runtime module name for this component container. Useful in resolving paths relative to the module
	 * @return the module name containing this component container
	 */
	public abstract String getRuntimeModuleName();

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
