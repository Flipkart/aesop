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
public class ElasticSearchInitializer implements InitializingBean{

    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchInitializer.class);

    public Client client;
    public Node node;

    private ElasticSearchConfig elasticSearchConfig;
    private Config config;

    public void init()
    {
        config = ConfigFactory.parseFile(new File(elasticSearchConfig.getConfig()));
        String hostname;
        try{
            hostname  = InetAddress.getLocalHost().getHostName();
        }
        catch(UnknownHostException e)
        {
            LOGGER.info("Unknown HostException Thrown - UpsertDatalayer");
            return;
        }

        /* Removing current host from host list so as to ensure that circular heart-beats are not sent. */
        List<String> hosts = (List<String>) config.getStringList("hosts");
        hosts.remove(hostname+":9300");
        String hostListStr = StringUtils.collectionToDelimitedString(hosts, ",");

        LOGGER.info("HOST LIST : {} ", hostListStr);
        Settings.Builder settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", config.getString("cluster.name")) /* Cluster Name Type : String */
                .put("http.port", 9200)
                .put("transport.tcp.port", 9300)
                .put("node.name", hostname.replace('.', '-'))
                .put("node.data", true)
                .put("node.local", config.getBoolean("isLocal")) /* Whether Its local Type : boolean */
                .put("path.work", config.getString("worklocation"))  /* Temp storage Location Type:String */
                .put("path.logs", config.getString("loglocation"))  /* Log Location Type:String */
                .put("path.data", config.getString("datalocation"))   /* Data Location Type:String */
                .put("index.number_of_shards", config.getInt("num.shards")) /* No Of Shards In 1 Node Type:Integer */
                .put("index.number_of_replicas", config.getInt("num.replicas"))  /* No Of Replicas In Cluster Type:Integer */
                .put("index.refresh_interval", config.getString("num.refresh_interval"))  /* No Of Replicas In Cluster Type:String */
                .put("discovery.zen.ping.multicast.enabled", false)
                .put("index.translog.flush_threshold_size","400mb")
                .put("indices.memory.index_buffer_size","25%")
                .put("threadpool.bulk.size",16)
                .put("threadpool.bulk.queue_size",100)
                .put("bootstrap.mlockall",true)
                .put("action.disable_delete_all_indices",true)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance",16)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries",32)
                .put("cluster.routing.allocation.node_concurrent_recoveries",4)
                .put("indices.recovery.concurrent_streams",8)
                .put("indices.recovery.max_bytes_per_sec","80mb")
                .put("discovery.zen.minimum_master_nodes", config.getInt("minimum_master_nodes"))
                .put("discovery.zen.ping.unicast.hosts", hostListStr)
                .put("network.host", hostname);

        Node node = nodeBuilder().local(Boolean.parseBoolean(String.valueOf(config.getBoolean("isLocal"))))
                .clusterName(String.valueOf(config.getString("cluster.name"))).data(true).client(false).settings(settings).node();

        node.client().admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        node.start() ;

        this.node = node;
        this.client = node.client();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
       init();
    }

    public ElasticSearchConfig getElasticSearchConfig() {
        return elasticSearchConfig;
    }

    public void setElasticSearchConfig(ElasticSearchConfig elasticSearchConfig) {
        this.elasticSearchConfig = elasticSearchConfig;
    }

    public String getIndex()
    {
        return  config.getString("cluster.index");
    }

    public String getNamespace()
    {
        return config.getString("cluster.namespace");
    }
}
