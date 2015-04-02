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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.flipkart.aesop.runtime.config.ProducerRegistration;
import com.flipkart.aesop.runtime.impl.registry.ServerContainerRegistry;
import com.flipkart.aesop.runtime.relay.DefaultRelay;
import com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;

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
     * Request handling for relays page
     */
    @RequestMapping(value = {"/relays","/"}, method = RequestMethod.GET)
    public String relays(ModelMap model, HttpServletRequest request) {

        // list of all relays and connected clients
    	List<RelayInfo> relayInfos = new LinkedList<RelayInfo>();
        for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
            if (DefaultRelay.class.isAssignableFrom(serverContainer.getClass())) {
                DefaultRelay relay = (DefaultRelay) serverContainer;
                // get all producers
                for (ProducerRegistration producerRegistration : relay.getProducerRegistrationList()){

                    RelayInfo relayInfo = new RelayInfo(
                            producerRegistration.getPhysicalSourceConfig().getId(),
                            producerRegistration.getPhysicalSourceConfig().getName(),
                            producerRegistration.getPhysicalSourceConfig().getUri()
                    );

                    // get all logical sources for the registered producer
                    RelayInfo.LSourceInfo[] lSourceInfos = this.getLogicalSourceForProducer(producerRegistration.getPhysicalSourceConfig().getSources());
                    relayInfo.setlSourceInfos(lSourceInfos);

                    // set producer name & SCN
                    relayInfo.setProducerName(producerRegistration.getEventProducer().getName());
                    relayInfo.setProducerSinceSCN(String.valueOf(producerRegistration.getEventProducer().getSCN()));

                    // now add connected clients details by getting the known connected clients from the Relay
                    List<String> peers = relay.getPeers();
                    RelayInfo.ClientInfo[] clientInfos = new RelayInfo.ClientInfo[peers.size()];
                    for (int i=0; i<clientInfos.length; i++) {

                        clientInfos[i] = new RelayInfo.ClientInfo(peers.get(i));
                        clientInfos[i].setClientSinceSCN(relay.getHttpStatisticsCollector()
                                .getPeerStats(peers.get(i))
                                .getMaxStreamWinScn());
                    }

                    // set all connected clients for the relay
                    relayInfo.setClientInfos(clientInfos);
                    // group the clients with their leading/trailing SCN
                    relayInfo.setHostGroupedClient();

                    relayInfos.add(relayInfo);
                }
            }
        }

        model.addAttribute("relayInfos", relayInfos.toArray(new RelayInfo[0]));
        if (request.getServletPath().endsWith(".json")) {
            return "relays-json";
        }

        // create Map object for json string required in the view to show expanded view of clients
        JSONObject relayClientGrouped = this.getRelayClientGroupedJson(relayInfos);
        model.addAttribute("relayClientGrouped", relayClientGrouped.toString());

        return "relays";
    }

    @RequestMapping(value = {"/metrics"}, method = RequestMethod.GET)
    public String metrics() {
        return "metrics";
    }

    /**
     * Request handling for metrics stream
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
     * Request handling for metrics snapshot
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
                    map.put("http", relay.getHttpStatisticsCollector());
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

    /**
     * Returns JSON structure required for UI to show expanded view of client partitions per client host
     * Structure of JSON { pId => { clientHost : [ { clientPartition : clienSCN }  ] } }
     * @param relayInfoList List of RelayInfo class
     * @return JSONObject grouped structure
     */
    private JSONObject getRelayClientGroupedJson(List<RelayInfo> relayInfoList) {
        Map<Integer, Map<String, Map<String, Long>>> relayClientGrouped = new HashMap<Integer,Map<String, Map<String, Long>>>();
        for(RelayInfo relay : relayInfoList) {

            Map<String, Map<String, Long>> relayClientInfo = relayClientGrouped.get(relay.getpSourceId());
            if (  relayClientInfo == null ) {
                relayClientInfo = new HashMap<String, Map<String, Long>>();
            }
            RelayInfo.ClientInfo[] clientInfos = relay.getClientInfos();
            for(RelayInfo.ClientInfo clientInfo : clientInfos) {
                String hostName = clientInfo.getClientHost();
                if(relayClientInfo.get(hostName) == null) {
                    relayClientInfo.put(
                            hostName , new HashMap<String, Long>()
                    );
                }
                relayClientInfo.get(hostName).put(
                        clientInfo.getClientName(), clientInfo.getClientSinceSCN()
                );
            }
            relayClientGrouped.put(relay.getpSourceId(), relayClientInfo);
        }

        return new JSONObject(relayClientGrouped);
    }

    /**
     * Create a list of LSourceInfo objects for the given lSources
     * @param lSources
     * @return
     */
    private RelayInfo.LSourceInfo[] getLogicalSourceForProducer(List<LogicalSourceConfig> lSources) {
        // take all the logical source that the producer will be registered with
        RelayInfo.LSourceInfo[] lSourceInfos = new RelayInfo.LSourceInfo[lSources.size()];
        for (int i=0; i < lSourceInfos.length; i++) {
            lSourceInfos[i] = new RelayInfo.LSourceInfo(lSources.get(i).getId());
            lSourceInfos[i].setLSourceName(lSources.get(i).getName());
            lSourceInfos[i].setLSourceURI(lSources.get(i).getUri());
        }

        return lSourceInfos;
    }
}
