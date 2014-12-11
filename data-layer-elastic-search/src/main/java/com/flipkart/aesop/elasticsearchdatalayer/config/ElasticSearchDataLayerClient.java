package com.flipkart.aesop.elasticsearchdatalayer.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.springframework.beans.factory.InitializingBean;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.springframework.util.StringUtils;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Initiates ElasticSearch Client , reads config from ElasticSearchConfig
 * @author Pratyay Banerjee
 */
public class ElasticSearchDataLayerClient implements InitializingBean
{
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDataLayerClient.class);

    /* This variable denotes the Elastic Search server client */
    private Client client;

    /* Elastic Search Config set by spring-beans */
    private ElasticSearchConfig elasticSearchConfig;

    /* Aesop Config Instance */
    private Config config;

    /**
     * This method is called from {@link org.springframework.beans.factory.InitializingBean#afterPropertiesSet()} to
     * initialize the Elastic Search Client
     */
    private void init()
    {
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

        /* Removing current host from host list so as to ensure that circular heart-beats are not sent. */
        List<String> hosts = config.getStringList("hosts");
        hosts.remove(hostname+":9300");
        String hostListStr = StringUtils.collectionToDelimitedString(hosts, ",");

        LOGGER.info("HOST LIST : {} ", hostListStr);
        Settings.Builder settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", config.getString("cluster.name")) /* Cluster Name Type : String */
                .put("node.name", hostname.replace('.', '-'))
                .put("node.data", false)
                .put("node.local", Boolean.parseBoolean(String.valueOf(config.getBoolean("isLocal")))) /* Whether Its local Type : boolean */
                .put("network.host", hostname);

        Node node = nodeBuilder().local(Boolean.parseBoolean(String.valueOf(config.getBoolean("isLocal"))))
                .clusterName(String.valueOf(config.getString("cluster.name"))).client(true).settings(settings).node();

        this.client = node.client();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
       init();
    }

    /* Getters and Setters Start */
    public ElasticSearchConfig getElasticSearchConfig() {
        return elasticSearchConfig;
    }

    public void setElasticSearchConfig(ElasticSearchConfig elasticSearchConfig) {
        this.elasticSearchConfig = elasticSearchConfig;
    }

    public String getIndex() {
        return  config.getString("cluster.index");
    }

    public String getType() {
        return config.getString("cluster.type");
    }

    public Client getClient() {
        return client;
    }
    /* Getters and Setters End */
}
