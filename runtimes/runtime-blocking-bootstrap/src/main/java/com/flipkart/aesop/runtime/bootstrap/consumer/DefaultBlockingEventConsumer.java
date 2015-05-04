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

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.eventconsumer.AbstractEventConsumer;
import com.google.common.base.Joiner;
import com.linkedin.databus2.core.BackoffTimer;
import org.springframework.beans.factory.InitializingBean;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * <code>DefaultBlockingEventConsumer</code> is the default blocking implementation for {@link SourceEventConsumer}. It
 * submits the events to the appropriate thread pool.
 * @author nrbafna
 */
public class DefaultBlockingEventConsumer implements SourceEventConsumer,InitializingBean
{
    public static final Logger LOGGER = LogFactory.getLogger(DefaultBlockingEventConsumer.class);

    private static final long DEFAULT_THREAD_AWAIT_TERMINATION_TIME_IN_SECS = 60;
    private final String PRIMARY_KEY_SEPERATOR = ";";
    /** executor pool for parallelizing consumption */
    private List<ThreadPoolExecutor> executors = new ArrayList<ThreadPoolExecutor>();
    /** No. Of Partitions to create  */
    private int numberOfPartition;
    /* Event Consumer to send events to */
    private AbstractEventConsumer eventConsumer;
    /* Back-Off Timer config */
    private BackoffTimer backoffTimer;
    /*
     * Thread Termination Max time. This config will ensure that threads
     * termination during shut-down is waiting for max these many secs
     * */
    private long threadTerminationMaxTime = DEFAULT_THREAD_AWAIT_TERMINATION_TIME_IN_SECS;
    private int executorQueueSize;
    private RejectedExecutionHandler rejectedExecutionHandler;

    @Override
    public void onEvent(AbstractEvent sourceEvent)
    {
        /** partition and submit */
        String primaryKeyValues = Joiner.on(PRIMARY_KEY_SEPERATOR).join(sourceEvent.getPrimaryKeyValues());
        Integer partitionNumber = ((primaryKeyValues.hashCode() & 0x7fffffff) % numberOfPartition);
        LOGGER.debug("Partition:" + primaryKeyValues.hashCode() + ":" + partitionNumber);
        executors.get(partitionNumber).execute(new SourceEventProcessor(sourceEvent, eventConsumer,backoffTimer));
    }

    public void shutdown()
    {
        for (int i = 0; i < numberOfPartition; i++) {
            executors.get(i).shutdown();
        }

        try
        {
            for (int i = 0; i < numberOfPartition; i++) {
                executors.get(i).awaitTermination(threadTerminationMaxTime, TimeUnit.NANOSECONDS);
            }
        }
        catch (InterruptedException e)
        {
            LOGGER.error("Error while stopping bootstrap consumer", e);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        this.numberOfPartition = Math.min(numberOfPartition, Runtime.getRuntime().availableProcessors());
        LOGGER.info("numberOfPartition used: " + numberOfPartition);
        for (int i = 0; i < numberOfPartition; i++)
        {
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(executorQueueSize);
            executors.add(new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue, rejectedExecutionHandler));
        }
    }

    /** Getters and Setters */
    public AbstractEventConsumer getEventConsumer() {
        return eventConsumer;
    }
    public void setEventConsumer(AbstractEventConsumer eventConsumer) {
        this.eventConsumer = eventConsumer;
    }
    public BackoffTimer getBackoffTimer() {
        return backoffTimer;
    }
    public void setBackoffTimer(BackoffTimer backoffTimer) {
        this.backoffTimer = backoffTimer;
    }
    public long getThreadTerminationMaxTime() {
        return threadTerminationMaxTime;
    }
    public void setThreadTerminationMaxTime(long threadTerminationMaxTime) {
        this.threadTerminationMaxTime = threadTerminationMaxTime;
    }
    public int getExecutorQueueSize() {
        return executorQueueSize;
    }
    public void setExecutorQueueSize(int executorQueueSize) {
        this.executorQueueSize = executorQueueSize;
    }
    public RejectedExecutionHandler getRejectedExecutionHandler() {
        return rejectedExecutionHandler;
    }
    public void setRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler) {
        this.rejectedExecutionHandler = rejectedExecutionHandler;
    }
    public int getNumberOfPartition() {
        return numberOfPartition;
    }
    public void setNumberOfPartition(int numberOfPartition) {
        this.numberOfPartition = numberOfPartition;
    }
}
