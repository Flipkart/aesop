package com.flipkart.aesop.runtime.bootstrap.consumer;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by nikhil.bafna on 2/2/15.
 */
public class WaitPolicy implements RejectedExecutionHandler
{
	private final long _time;
	private final TimeUnit _timeUnit;

	public WaitPolicy()
	{
		this(Long.MAX_VALUE, TimeUnit.SECONDS);
	}

	public WaitPolicy(long time, TimeUnit timeUnit)
	{
		super();
		_time = (time < 0 ? Long.MAX_VALUE : time);
		_timeUnit = timeUnit;
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor e)
	{
		try
		{
			while (!e.isShutdown())
			{
				e.getQueue().offer(r, _time, _timeUnit);
			}
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RejectedExecutionException(ie);
		}
	}
}
