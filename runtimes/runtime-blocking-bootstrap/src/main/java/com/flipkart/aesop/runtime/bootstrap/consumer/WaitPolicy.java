/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.aesop.runtime.bootstrap.consumer;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <code>WaitPolicy</code> is a waiting implementation of {@link RejectedExecutionHandler}. It tries to put the runnable
 * into the queue in loop, breaking when the executor is shutdown
 * @author nrbafna
 */
public class WaitPolicy implements RejectedExecutionHandler
{
	private final long _time;
	private final TimeUnit _timeUnit;

	public WaitPolicy()
	{
		this(1, TimeUnit.SECONDS);
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
			while (!e.isShutdown() && !e.getQueue().offer(r, _time, _timeUnit))
			{
			}
		}
		catch (InterruptedException ie)
		{
			Thread.currentThread().interrupt();
			throw new RejectedExecutionException(ie);
		}
	}
}
