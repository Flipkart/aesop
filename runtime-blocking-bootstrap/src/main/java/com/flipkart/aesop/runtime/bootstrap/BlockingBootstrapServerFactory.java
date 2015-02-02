package com.flipkart.aesop.runtime.bootstrap;

import org.springframework.beans.factory.FactoryBean;

import com.flipkart.aesop.runtime.bootstrap.configs.BootstrapConfig;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.producers.EventProducer;

/**
 * Created by nikhil.bafna on 2/2/15.
 */
public class BlockingBootstrapServerFactory implements FactoryBean<BlockingBootstrapServer>
{
	private BootstrapConfig bootstrapConfig;
	private EventProducer producer;

	@Override
	public BlockingBootstrapServer getObject() throws Exception
	{
		ServerContainer.Config config = new ServerContainer.Config();
		ConfigLoader<ServerContainer.StaticConfig> configLoader =
		        new ConfigLoader<ServerContainer.StaticConfig>(BootstrapConfig.BOOTSTRAP_PROPERTIES_PREFIX, config);

		ServerContainer.StaticConfig staticConfig =
		        configLoader.loadConfig(this.bootstrapConfig.getBootstrapProperties());
		BlockingBootstrapServer bootstrapServer = new BlockingBootstrapServer(staticConfig);
		bootstrapServer.registerProducer(producer);
		return bootstrapServer;
	}

	@Override
	public Class<?> getObjectType()
	{
		return BlockingBootstrapServer.class;
	}

	@Override
	public boolean isSingleton()
	{
		return true;
	}

	public void setBootstrapConfig(BootstrapConfig bootstrapConfig)
	{
		this.bootstrapConfig = bootstrapConfig;
	}

	public void setProducer(EventProducer producer)
	{
		this.producer = producer;
	}
}
