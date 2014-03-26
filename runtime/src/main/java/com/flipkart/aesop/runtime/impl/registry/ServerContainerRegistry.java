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

package com.flipkart.aesop.runtime.impl.registry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry;
import com.flipkart.aesop.runtime.spring.registry.ServerContainerConfigInfo;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * Implementation of {@link AbstractRuntimeRegistry} for ServerContainer instances
 *
 * @author Regunath B
 * @version 1.0, 06 Jan 2014
 */
public class ServerContainerRegistry implements AbstractRuntimeRegistry {

    /** logger */
    private static Logger LOGGER = LogFactory.getLogger(ServerContainerRegistry.class);

    /** list of runtime instances by name */
    private Map<String,ServerContainer> runtimes = new HashMap<String,ServerContainer>();

	/**
	 * Interface method implementation. 
	 * @see com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry#init(java.util.List)
	 */
	public InitedRuntimeInfo[] init(List<ServerContainerConfigInfo> serverContainerConfigList) throws Exception {
    	List<AbstractRuntimeRegistry.InitedRuntimeInfo> initedRuntimeInfos = new LinkedList<AbstractRuntimeRegistry.InitedRuntimeInfo>();
        for (ServerContainerConfigInfo serverContainerConfigInfo : serverContainerConfigList) {
            String[] runtimeBeanIds = serverContainerConfigInfo.getRuntimeContext().getBeanNamesForType(ServerContainer.class);
            for (String runtimeBeanId : runtimeBeanIds) {
            	ServerContainer runtime = (ServerContainer) serverContainerConfigInfo.getRuntimeContext().getBean(runtimeBeanId);
                try {
                    LOGGER.info("Initializing ServerContainer: " + runtime.getComponentAdmin().getComponentName());
                    runtime.start();
                    initedRuntimeInfos.add(new AbstractRuntimeRegistry.InitedRuntimeInfo(runtime,serverContainerConfigInfo));
                } catch (Exception e) {
                    LOGGER.error("Error initializing ServerContainer {}. Error is: " + e.getMessage(), runtime.getComponentAdmin().getComponentName(), e);
                    throw new PlatformException("Error initializing ServerContainer: " + runtime.getComponentAdmin().getComponentName(), e);
                }
                this.runtimes.put(runtime.getComponentAdmin().getComponentName(),runtime);
            }
        }
        return initedRuntimeInfos.toArray(new AbstractRuntimeRegistry.InitedRuntimeInfo[0]);
	}

	/**
	 * Interface method implementation.
	 * @see com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry#reinitRuntime(java.lang.String)
	 */
	public void reinitRuntime(String name) throws Exception {
		ServerContainer runtime = this.runtimes.get(name);
        if (runtime != null) {
            try {
            	runtime.shutdown();
            	runtime.start();
            } catch (Exception e) {
                LOGGER.error("Error initializing ServerContainer {}. Error is: " + e.getMessage(), name, e);
                throw new PlatformException("Error reinitialising ServerContainer: " + name, e);
            }
        }		
	}

	/**
	 * Interface method implementation
	 * @see com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry#shutdown()
	 */
	public void shutdown() throws Exception {
        for (String name : this.runtimes.keySet()) {
            try {
                LOGGER.info("Shutting down ServerContainer: " + name);
                this.runtimes.get(name).shutdown();
            } catch (Exception e) {
                LOGGER.warn("Failed to shutdown ServerContainer: " + name, e);
            }
        }
	}

	/**
	 * Interface method implementation
	 * @see com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry#getRuntimes()
	 */
	public List<ServerContainer> getRuntimes() {
        return new ArrayList<ServerContainer>(this.runtimes.values());
	}

	/**
	 * Interface method implementation
	 * @see com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry#getRuntime(java.lang.String)
	 */
	public ServerContainer getRuntime(String name) {
		return this.runtimes.get(name);
	}

	/**
	 * Interface method implementation
	 * @see com.flipkart.aesop.runtime.spi.registry.AbstractRuntimeRegistry#unregisterRuntime(com.linkedin.databus2.core.container.netty.ServerContainer)
	 */
	public void unregisterRuntime(ServerContainer runtime) {
		this.runtimes.remove(runtime.getComponentAdmin().getComponentName());
	}

}
