package com.flipkart.aesop.runtime.bootstrap;

import java.io.IOException;
import java.nio.ByteOrder;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.monitoring.mbean.DatabusComponentAdmin;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.producers.EventProducer;

/**
 * Created by nikhil.bafna on 1/28/15.
 */
public class BlockingBootstrapServer extends ServerContainer
{
	private EventProducer producer;

	public BlockingBootstrapServer() throws DatabusException, IOException
	{
		super(new Config().build(), ByteOrder.BIG_ENDIAN);
	}

	public BlockingBootstrapServer(StaticConfig staticConfig) throws DatabusException, IOException
	{
		super(staticConfig, ByteOrder.BIG_ENDIAN);
	}

	@Override
	protected DatabusComponentAdmin createComponentAdmin()
	{
		return new DatabusComponentAdmin(this, getMbeanServer(), HttpRelay.class.getSimpleName());
	}

	@Override
	public void pause()
	{
		getComponentStatus().pause();
		producer.pause();
	}

	@Override
	public void resume()
	{
		getComponentStatus().resume();
		producer.unpause();
	}

	@Override
	public void suspendOnError(Throwable throwable)
	{
		getComponentStatus().suspendOnError(throwable);
	}

	@Override
	protected void doStart()
	{
		super.doStart();
		producer.start(0);
	}

	@Override
	protected void doShutdown()
	{
		super.doShutdown();
		producer.shutdown();
	}

	public void registerProducer(EventProducer producer)
	{
		this.producer = producer;
	}
}
