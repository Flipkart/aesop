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

import com.flipkart.aesop.runtime.bootstrap.BlockingBootstrapServer;
import com.flipkart.aesop.runtime.bootstrap.producer.registeration.ProducerRegistration;
import com.flipkart.aesop.runtime.impl.registry.ServerContainerRegistry;
import com.flipkart.aesop.runtime.spi.admin.RuntimeConfigService;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The <code>BlockingBootstrapController</code> class is a Spring MVC Controller that displays Blocking Bootstrap Metrics.
 */
@Controller
public class BlockingBootstrapProducerController
{

    /** max connections for the stream */
    private static final int MAX_CONNECTIONS = 5;

    private static Logger logger = LoggerFactory.getLogger(BlockingBootstrapProducerController.class);

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
        List<BlockingBootstrapInfo> blockingBootstrapInfos = new LinkedList<BlockingBootstrapInfo>();
        for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
            if (BlockingBootstrapServer.class.isAssignableFrom(serverContainer.getClass())) {
                BlockingBootstrapServer blockingBootstrapServer = (BlockingBootstrapServer) serverContainer;
                // get all producers
                for (ProducerRegistration producerRegistration : blockingBootstrapServer.getProducerRegistrationList()){

                    BlockingBootstrapInfo blockingBootstrapInfo = new BlockingBootstrapInfo(
                            producerRegistration.getPhysicalSourceConfig().getId(),
                            producerRegistration.getPhysicalSourceConfig().getName(),
                            producerRegistration.getPhysicalSourceConfig().getUri()
                    );

                    // get all logical sources for the registered producer
                    BlockingBootstrapInfo.LSourceInfo[] lSourceInfos = this.getLogicalSourceForProducer(producerRegistration.getPhysicalSourceConfig().getSources());
                    blockingBootstrapInfo.setlSourceInfos(lSourceInfos);

                    // set producer name & SCN
                    blockingBootstrapInfo.setProducerName(producerRegistration.getEventProducer().getName());
                    blockingBootstrapInfo.setProducerSinceSCN(String.valueOf(producerRegistration.getEventProducer().getSCN()));

                    // now add connected clients details by getting the known connected clients from the Relay
                    blockingBootstrapInfos.add(blockingBootstrapInfo);
                }
            }
        }

        model.addAttribute("relayInfos", blockingBootstrapInfos.toArray(new BlockingBootstrapInfo[0]));
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
                BlockingBootstrapServer blockingBootstrapServer = null;
                for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
                    if (BlockingBootstrapServer.class.isAssignableFrom(serverContainer.getClass())) {
                        blockingBootstrapServer = (BlockingBootstrapServer) serverContainer;
                        break;
                    }
                }

                if (blockingBootstrapServer != null) {
                    logger.info("Client connected: " + request.getSession().getId());
                    // set appropriate headers for a stream
                    response.setHeader("Content-Type", "text/event-stream;charset=UTF-8");
                    response.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
                    response.setHeader("Pragma", "no-cache");
                    // loop metrics
                    while (true) {
                        response.getWriter().println("data: " + blockingBootstrapServer.getMetricsCollector().getJson() + "\n");
                        response.flushBuffer();
                        Thread.sleep(blockingBootstrapServer.getMetricsCollector().getRefreshInterval() * 1000);
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
            BlockingBootstrapServer blockingBootstrapServer = null;
            for (ServerContainer serverContainer : this.runtimeRegistry.getRuntimes()) {
                if (BlockingBootstrapServer.class.isAssignableFrom(serverContainer.getClass())) {
                    blockingBootstrapServer = (BlockingBootstrapServer) serverContainer;
                    break;
                }
            }
            if (blockingBootstrapServer != null) {
                if (request.getParameterMap().containsKey("full")) {
                    Map<String,Object> map = new HashMap<String, Object>();
                    map.put("inbound", blockingBootstrapServer.getInboundEventStatisticsCollector());
                    map.put("outbound", blockingBootstrapServer.getOutboundEventStatisticsCollector());
                    map.put("http", blockingBootstrapServer.getHttpStatisticsCollector());
                    response.getWriter().print(mapper.writeValueAsString(map));
                } else {
                    response.getWriter().print(blockingBootstrapServer.getMetricsCollector().getJson());
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
     * Create a list of LSourceInfo objects for the given lSources
     * @param lSources
     * @return
     */
    private BlockingBootstrapInfo.LSourceInfo[] getLogicalSourceForProducer(List<LogicalSourceConfig> lSources) {
        // take all the logical source that the producer will be registered with
        BlockingBootstrapInfo.LSourceInfo[] lSourceInfos = new BlockingBootstrapInfo.LSourceInfo[lSources.size()];
        for (int i=0; i < lSourceInfos.length; i++) {
            lSourceInfos[i] = new BlockingBootstrapInfo.LSourceInfo(lSources.get(i).getId());
            lSourceInfos[i].setLSourceName(lSources.get(i).getName());
            lSourceInfos[i].setLSourceURI(lSources.get(i).getUri());
        }

        return lSourceInfos;
    }
}
