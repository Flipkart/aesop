package com.flipkart.aesop.processor.kafka.config;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Initiates Kafka Client , reads config from KafkaConfig
 * @author Ravindra Yadav
 */
public class KafkaDataLayerClient implements InitializingBean
{
	private static final Logger LOGGER = LogFactory.getLogger(KafkaDataLayerClient.class);

	/* This variable denotes the Kafka server client */
	// private Client client;

	/* Kafka Config set by spring-beans */
	private KafkaConfig kafkaConfig;

	/* Aesop Config Instance */
	private Config config;

	/**
	 * This method is called from {@link org.springframework.beans.factory.InitializingBean#afterPropertiesSet()} to
	 * initialize the Kafka Client
	 */
	private void init()
	{
		this.config = ConfigFactory.parseFile(new File(kafkaConfig.getConfig()));
		String hostname;
		try
		{
			hostname = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e)
		{
			LOGGER.info("Unknown HostException Thrown - UpsertDatalayer");
			throw new RuntimeException("FATAL Error: Unable To get Host Information");
		}

		/* Removing current host from host list so as to ensure that circular heart-beats are not sent. */
		List<String> hosts = config.getStringList("hosts");
		hosts.remove(hostname + ":9300");
		String hostListStr = StringUtils.collectionToDelimitedString(hosts, ",");

		LOGGER.info("HOST LIST : {} ", hostListStr);

		// Settings.Builder settings =
		// ImmutableSettings.settingsBuilder().put("cluster.name", config.getString("cluster.name"))
		// .put("node.name", hostname.replace('.', '-')).put("network.host", hostname)
		// .put("node.data", false).put("node.local", false);
		//
		// Node node =
		// nodeBuilder().clusterName(config.getString("cluster.name")).client(true).local(false)
		// .settings(settings).node();
		// node.start();
		//
		// this.client = node.client();
	}

	public void afterPropertiesSet() throws Exception
	{
		init();
	}

	/* Getters and Setters Start */
	public KafkaConfig getKafkaConfig()
	{
		return kafkaConfig;
	}

	public void setKafkaConfig(KafkaConfig kafkaConfig)
	{
		this.kafkaConfig = kafkaConfig;
	}

	public String getIndex()
	{
		return config.getString("cluster.index");
	}

	public String getType()
	{
		return config.getString("cluster.type");
	}

	// public Client getClient()
	// {
	// return client;
	// }
	/* Getters and Setters End */
}
