package com.flipkart.aesop.elasticsearchdatalayer.delete;

import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchInitializer;
import com.typesafe.config.Config;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.springframework.util.StringUtils;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
/**
 * Sample Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to Log File.
 * @author Jagadeesh Huliyar
 * @see com.flipkart.aesop.elasticsearchdatalayer.upsert.ElasticSearchUpsertDataLayer
 */
public class ElasticSearchDeleteDataLayer extends DeleteDestinationStoreOperation
{

    /** Logger for this class*/
    private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchDeleteDataLayer.class);
    // private static final DialServer ds = new DialServer();


    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ElasticSearchInitializer elasticSearchInitializer;

	@Override
	protected void delete(AbstractEvent event)
	{
		LOGGER.info("Received Delete Event. Event is " + event);
        String id = String.valueOf(event.getFieldMapPair().get("id"));
        /* Prepare Delete Request and exeucte */
        elasticSearchInitializer.client.prepareDelete("ortest_person", "person",id)
                .execute()
                .actionGet();

            /* Check if source still exists*/
        try{
        GetResponse response = elasticSearchInitializer.client.prepareGet("ortest_person", "person", id).execute().get();
        if(!response.isSourceEmpty()) {
            LOGGER.info("Delete Error:" + response);
        }
        }
        catch(Exception e)
        {
            LOGGER.info("Delete Error:" + e);
        }

	}
}
