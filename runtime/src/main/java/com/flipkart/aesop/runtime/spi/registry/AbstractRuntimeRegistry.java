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

package com.flipkart.aesop.runtime.spi.registry;

import java.util.List;


import com.flipkart.aesop.runtime.spring.registry.ServerContainerConfigInfo;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * Interface for runtime registry. Holds information on loaded runtime instances and provides methods to query them.
 * Also controls lifecycle methods of all runtime instances understood by this registry.
 *
 * @author Regunath B
 * @version 1.0, 6 Jan 2014
 */
public interface AbstractRuntimeRegistry {

    /**
     * Lifecycle init method. Initializes all individual runtime instances understood by this registry.
     * @param serverContainerConfigList List of ServerContainerConfig to be initialized
     * @return array of AbstractRuntimeRegistry.InitedRuntimeInfo instances for each inited runtime
     * @throws Exception in case of errors during initialization
     */
    public AbstractRuntimeRegistry.InitedRuntimeInfo[] init(List<ServerContainerConfigInfo> serverContainerConfigList) throws Exception;

    /**
     * Method to reinitialize a runtime.
     * @param name Name of the runtime.
     * @throws Exception in case of errors during re-init
     */
    public void reinitRuntime(String name) throws Exception;

    /**
     * Lifecycle shutdown method. Shuts down all individual runtimes loaded by this registry.
     * @throws Exception in case of errors during shutdown
     */
    public void shutdown() throws Exception;

    /**
     * Enumeration method for all runtimes. Returns a List of ServerContainer instances
     * @return List
     */
    public List<ServerContainer> getRuntimes();

    /**
     * Get a runtime given name
     * @param name String name of the runtime
     * @return ServerContainer
     */
    public ServerContainer getRuntime(String name);
    
	/**
	 * Unregisters (removes) a ServerContainer from this registry.
	 * @param runtime the ServerContainer to be removed
	 */
    public void unregisterRuntime(ServerContainer runtime);
    
    /**
     *  Container object for inited runtimes and the respective configuration
     */
    public static final class InitedRuntimeInfo {
    	private ServerContainer initedRuntime;
    	private ServerContainerConfigInfo runtimeConfigInfo;
    	public InitedRuntimeInfo(ServerContainer initedRuntime, ServerContainerConfigInfo runtimeConfigInfo) {
    		this.initedRuntime = initedRuntime;
    		this.runtimeConfigInfo = runtimeConfigInfo;
    	}
		public ServerContainer getInitedRuntime() {
			return initedRuntime;
		}
		public ServerContainerConfigInfo getRuntimeConfigInfo() {
			return this.runtimeConfigInfo;
		}
    }

}
