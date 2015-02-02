package com.flipkart.aesop.runtime.bootstrap.consumer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yogesh.dahiya
 */

public class BoundedThreadPoolExecutor
{
	private ThreadPoolExecutor executor;

	public BoundedThreadPoolExecutor(int poolSize, int queueSize)
	{
		BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(queueSize);
		this.executor =
		        new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, queue,
		                new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public void submit(Runnable r)
	{
		executor.execute(r);
	}

	public void shutdown() throws InterruptedException
	{
		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	}

}
