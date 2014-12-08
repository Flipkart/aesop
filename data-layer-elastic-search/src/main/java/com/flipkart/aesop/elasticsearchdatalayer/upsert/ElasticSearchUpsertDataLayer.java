package com.flipkart.aesop.elasticsearchdatalayer.upsert;


import com.flipkart.aesop.elasticsearchdatalayer.config.ElasticSearchInitializer;
import com.typesafe.config.Config;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreOperation;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.core.DbusOpcode;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;




import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
/**
 * Sample Upsert Data Layer. Persists {@link DbusOpcode#UPSERT} events to Logs.
 * @author Jagadeesh Huliyar
 * @see com.flipkart.aesop.elasticsearchdatalayer.delete.ElasticSearchDeleteDataLayer
 */
public class ElasticSearchUpsertDataLayer extends UpsertDestinationStoreOperation
{
	/** Logger for this class*/
	private static final Logger LOGGER = LogFactory.getLogger(ElasticSearchUpsertDataLayer.class);
   // private static final DialServer ds = new DialServer();


    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /* ES Node Client. */

    public ElasticSearchInitializer elasticSearchInitializer;

	@Override
	protected void upsert(AbstractEvent event)
	{
        //ds.checkServer();
		LOGGER.info("Received Upsert Event. Event is " + event);
        LOGGER.info("Field Map Pair : " + event.getFieldMapPair().toString());
        LOGGER.info("Person ID:" + event.getFieldMapPair().get("id"));


        try {
            String id = String.valueOf(event.getFieldMapPair().get("id"));
            elasticSearchInitializer.client.prepareDelete("ortest_person","person",id)
                    .execute()
                    .actionGet();
            IndexResponse response = elasticSearchInitializer.client.prepareIndex("ortest_person","person",id)
                    .setSource(event.getFieldMapPair())
                    .execute()
                    .get();
            if(!response.isCreated())  {
                LOGGER.info("Create Error : " + response);
            }
        } catch (Exception e) {
            LOGGER.info("Create Error : " + e);
        }

	}
}
