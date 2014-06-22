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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.flipkart.aesop.runtime.config.ProducerRegistration;
import com.flipkart.aesop.runtime.impl.registry.ServerContainerRegistry;
import com.flipkart.aesop.runtime.relay.DefaultRelay;
import com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The <code>RelayController</code> class is a Spring MVC Controller that displays Relay Metrics. Also provides
 * functionality to edit relay configurations and re-initialize them
 * 
 * @author Regunath B
 * @version 1.0, 12 May 2014 
 */

@Controller
public class RelayController {

    /** max connections for the stream */
    private static final int MAX_CONNECTIONS = 5;

    /** refresh interval */
    private static final int REFRESH_INTERVAL_SECONDS = 5;

    private static Logger logger = LoggerFactory.getLogger(RelayController.class);

    /** The ServerContainerRegsitry instance for accessing all deployed Relay instances*/
	private ServerContainerRegistry runtimeRegistry;
	
	/** The RuntimeConfigService instance for administration actions on individual deployed Relay instances*/
	private RuntimeConfigService configService;

    /** Object mapper for json conversion */
    private ObjectMapper mapper = new ObjectMapper();

    /** counter for concurrent connections for the stream */
    private static AtomicInteger concurrentConnections = new AtomicInteger(0);

    /**
     * Controller for relays page
     */
    @RequestMapping(value = {"/relays","/"}, method = RequestMethod.GET)
    public String relays(ModelMap model, HttpServletRequest request) {
    	List<RelayInfo> relayInfos = new LinkedList<RelayInfo>();
        for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
            if (DefaultRelay.class.isAssignableFrom(serverContainer.getClass())) {
                DefaultRelay relay = (DefaultRelay) serverContainer;
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
        if (request.getServletPath().endsWith(".json")) {
            return "relays-json";
        }
        return "relays";
    }

    @RequestMapping(value = {"/metrics"}, method = RequestMethod.GET)
    public String metrics() {
        return "metrics";
    }

    /**
     * Controller for relays page
     */
    @RequestMapping(value = {"/metrics-stream"}, method = RequestMethod.GET)
    public @ResponseBody void metricsStream(HttpServletRequest request, HttpServletResponse response) {

        try {

            // restrict max concurrency
            if (concurrentConnections.incrementAndGet() > MAX_CONNECTIONS) {

                logger.info("Client refused due to max concurrency reached");
                response.sendError(503, "Max concurrent connections reached: " + MAX_CONNECTIONS);

            } else {


                // find the relay
                DefaultRelay relay = null;
                for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
                    if (DefaultRelay.class.isAssignableFrom(serverContainer.getClass())) {
                        relay = (DefaultRelay) serverContainer;
                        break;
                    }
                }

                if (relay != null) {

                    logger.info("Client connected: " + request.getSession().getId());

                    // set appropriate headers for a stream
                    response.setHeader("Content-Type", "text/event-stream;charset=UTF-8");
                    response.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
                    response.setHeader("Pragma", "no-cache");

                    // loop metrics
                    while (true) {
                        response.getWriter().println("data: " + relay.getMetricsCollector().getJson() + "\n");
                        response.flushBuffer();
                        Thread.sleep(relay.getMetricsCollector().getRefreshInterval() * 1000);
                    }

                } else {
                    logger.info("Relay not found!");
                    response.sendError(404,"Relay not found!");
                }

            }

        } catch (IOException e) {
            logger.info("Client Disconnected: " + request.getSession().getId());
        } catch (InterruptedException e) {
            logger.info("Client Disconnected: " + request.getSession().getId() + " (Interrupted)");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Client Disconnected: " + request.getSession().getId() + " (Unknown Exception)", e);
        } finally {
            concurrentConnections.decrementAndGet();
        }

    }

    /**
     * Controller for relays page
     */
    @RequestMapping(value = {"/metrics-json"}, method = RequestMethod.GET)
    public @ResponseBody void metricsJSON(HttpServletRequest request, HttpServletResponse response) {

        try {

            // set appropriate headers for a stream
            response.setHeader("Content-Type", "application/json");
            response.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
            response.setHeader("Pragma", "no-cache");

            DefaultRelay relay = null;
            for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
                if (DefaultRelay.class.isAssignableFrom(serverContainer.getClass())) {
                    relay = (DefaultRelay) serverContainer;
                    break;
                }
            }

            if (relay != null) {
                if (request.getParameterMap().containsKey("full")) {
                    Map<String,Object> map = new HashMap<String, Object>();
                    map.put("inbound", relay.getInboundEventStatisticsCollector());
                    map.put("outbound", relay.getOutboundEventStatisticsCollector());
                    map.put("http",relay.getHttpStatisticsCollector());
                    response.getWriter().print(mapper.writeValueAsString(map));
                } else {
                    response.getWriter().print(relay.getMetricsCollector().getJson());
                }
            } else {
                response.getWriter().println("{}");
            }
            response.flushBuffer();

        } catch (IOException e) {
            logger.info("Client Disconnected: " + request.getSession().getId());
        } catch (Exception e) {
            logger.error("Client Disconnected: " + request.getSession().getId() + " (Unknown Exception)", e);
        }

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
