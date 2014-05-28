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
package com.flipkart.aesop.runtime.spring.web;

import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.flipkart.aesop.runtime.config.ProducerRegistration;
import com.flipkart.aesop.runtime.impl.registry.ServerContainerRegistry;
import com.flipkart.aesop.runtime.relay.DefaultRelay;
import com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * The <code>RelayController</code> class is a Spring MVC Controller that displays Relay Metrics. Also provides
 * functionality to edit relay configurations and re-initialize them
 * 
 * @author Regunath B
 * @version 1.0, 12 May 2014 
 */

@Controller
public class RelayController {
	
	/** The ServerContainerRegsitry instance for accessing all deployed Relay instances*/
	private ServerContainerRegistry runtimeRegistry;
	
	/** The RuntimeConfigService instance for administration actions on individual deployed Relay instances*/
	private RuntimeConfigService configService;

    /**
     * Controller for relays page
     */
    @RequestMapping(value = {"/relays","/"}, method = RequestMethod.GET)
    public String relays(ModelMap model, HttpServletRequest request) {
    	List<RelayInfo> relayInfos = new LinkedList<RelayInfo>();
    	for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
    		if (DefaultRelay.class.isAssignableFrom(serverContainer.getClass())) {
    			DefaultRelay relay = (DefaultRelay)serverContainer;
    			for (ProducerRegistration producerRegistration : relay.getProducerRegistrationList()){
    				RelayInfo relayInfo = new RelayInfo(producerRegistration.getPhysicalSourceConfig().getId(), 
    						producerRegistration.getPhysicalSourceConfig().getName(), producerRegistration.getPhysicalSourceConfig().getUri());
    				if (producerRegistration.getPhysicalSourceConfig().getSources().size() > 0) {
    					// take only the first logical source that the producer will be registered with
    					relayInfo.setlSourceId(producerRegistration.getPhysicalSourceConfig().getSources().get(0).getId());
    					relayInfo.setlSourceName(producerRegistration.getPhysicalSourceConfig().getSources().get(0).getName());
    					relayInfo.setlSourceURI(producerRegistration.getPhysicalSourceConfig().getSources().get(0).getUri());
    				}
    				relayInfo.setProducerName(producerRegistration.getEventProducer().getName());
    				relayInfo.setProducerSinceSCN(String.valueOf(producerRegistration.getEventProducer().getSCN()));    				
    				// now add connected clients details
    				List<String> peers = relay.getHttpStatisticsCollector().getPeers();
    				RelayInfo.ClientInfo[] clientInfos = new RelayInfo.ClientInfo[peers.size()];
    				for (int i=0; i<clientInfos.length;i++) {
    					clientInfos[i] = new RelayInfo.ClientInfo(peers.get(i));
    					clientInfos[i].setClientSinceSCN(String.valueOf(relay.getHttpStatisticsCollector().getPeerStats(
    							peers.get(i)).getMaxStreamWinScn())); 
    				}
    				relayInfo.setClientInfos(clientInfos);
    				relayInfos.add(relayInfo);
    			}
    		}
    	}
        model.addAttribute("relayInfos",relayInfos.toArray(new RelayInfo[0]));
        if(request.getServletPath().endsWith(".json")) {
            return "relays-json";
        }
        return "relays";
    }

    /** Getter Setter methods */
	public ServerContainerRegistry getRuntimeRegistry() {
		return runtimeRegistry;
	}
	public void setRuntimeRegistry(ServerContainerRegistry runtimeRegistry) {
		this.runtimeRegistry = runtimeRegistry;
	}
	public RuntimeConfigService getConfigService() {
		return configService;
	}
	public void setConfigService(RuntimeConfigService configService) {
		this.configService = configService;
	}

}
