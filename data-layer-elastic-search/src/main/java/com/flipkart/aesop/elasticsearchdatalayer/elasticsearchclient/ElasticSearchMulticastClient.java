/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *******************************************************************************/
package com.flipkart.aesop.elasticsearchdatalayer.elasticsearchclient;

import com.typesafe.config.ConfigFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Initiates ElasticSearchMulticast Client , uses the MulticastClient of elasticSearch master server discovery
 * @author Pratyay Banerjee
 */
public class ElasticSearchMulticastClient extends ElasticSearchClient {

    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchMulticastClient.class);

    @Override
    void init() {
        this.config = ConfigFactory.parseFile(new File(elasticSearchConfig.getConfig()));
        String hostname;
        try{
            hostname  = InetAddress.getLocalHost().getHostName();
        }
        catch(UnknownHostException e)
        {
            LOGGER.info("Unknown HostException Thrown - UpsertDatalayer");
            throw new RuntimeException("FATAL Error: Unable To get Host Information");
        }

        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", config.getString("cluster.name"))
                .put("node.name", hostname.replace('.', '-'))
                .put("network.host", hostname)
                .put("node.data",false)
                .put("node.local",false)
                .put("discovery.zen.ping.multicast.group", config.getString("multicast.groupHost"))
                .put("discovery.zen.ping.multicast.port", config.getInt("multicast.groupPort"))
                .put("discovery.zen.ping.multicast.ttl", 10)
                .put("discovery.zen.ping.multicast.address", hostname)
                .put("discovery.zen.fd.ping_interval", 3)
                .put("discovery.zen.fd.ping_timeout", 5)
                .put("discovery.zen.fd.ping_retries", 100)
                .put("network.host", hostname);


        Node node = nodeBuilder().clusterName(config.getString("cluster.name")).client(true).local(false).settings(settings).node();
        node.start();
        this.client = node.client();
    }
}
