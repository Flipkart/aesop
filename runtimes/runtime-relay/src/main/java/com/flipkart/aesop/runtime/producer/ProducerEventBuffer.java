/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.producer;

import com.flipkart.aesop.runtime.metrics.MetricsCollector;
import com.linkedin.databus.core.*;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

/**
 * Wrapper for the Databus event buffer that uses a {@link MetricsCollector} to tracking producer SCNs
 * @author kartikbu
 * @created 10/07/14
 */
public class ProducerEventBuffer implements DbusEventBufferAppendable {

	/** The producer name*/
    private String producerName;

    /** The event buffer that is wrapped by this event buffer*/
    private DbusEventBufferAppendable eventBuffer;

    /** The metrics collector for tracking producer SCNs*/
    private MetricsCollector collector;

    /**
     * Constructor for this class
     * @param producerName the name of the producer
     * @param eventBuffer the event buffer that holds event data
     * @param collector the metrics collector instance
     */
    public ProducerEventBuffer(String producerName, DbusEventBufferAppendable eventBuffer, MetricsCollector collector) {
        this.producerName = producerName;
        this.eventBuffer = eventBuffer;
        this.collector = collector;
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#start(long)
     */
    public void start(long l) {
        eventBuffer.start(l);
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#startEvents()
     */
    public void startEvents() {
        eventBuffer.startEvents();
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#appendEvent(com.linkedin.databus.core.DbusEventKey, long, short, short, long, short, byte[], byte[], boolean, com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector)
     */
    public boolean appendEvent(DbusEventKey dbusEventKey, long l, short i, short i2, long l2, short i3, byte[] bytes, byte[] bytes2, boolean b, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey, l, i, i2, l2, i3, bytes, bytes2, b, statisticsCollector);
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#appendEvent(com.linkedin.databus.core.DbusEventKey, short, short, long, short, byte[], byte[], boolean)
     */
    public boolean appendEvent(DbusEventKey dbusEventKey, short i, short i2, long l, short i3, byte[] bytes, byte[] bytes2, boolean b) {
        return eventBuffer.appendEvent(dbusEventKey, i, i2, l, i3, bytes, bytes2, b);
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#appendEvent(com.linkedin.databus.core.DbusEventKey, short, short, long, short, byte[], byte[], boolean, com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector)
     */
    public boolean appendEvent(DbusEventKey dbusEventKey, short i, short i2, long l, short i3, byte[] bytes, byte[] bytes2, boolean b, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey,i,i2,l,i3,bytes,bytes2,b,statisticsCollector);
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#appendEvent(com.linkedin.databus.core.DbusEventKey, short, short, long, short, byte[], byte[], boolean, boolean, com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector)
     */
    public boolean appendEvent(DbusEventKey dbusEventKey, short i, short i2, long l, short i3, byte[] bytes, byte[] bytes2, boolean b, boolean b2, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey, i, i2, l, i3, bytes, bytes2, b, b2, statisticsCollector);
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#appendEvent(com.linkedin.databus.core.DbusEventKey, com.linkedin.databus.core.DbusEventInfo, com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector)
     */
    public boolean appendEvent(DbusEventKey dbusEventKey, DbusEventInfo dbusEventInfo, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey, dbusEventInfo, statisticsCollector);
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#rollbackEvents()
     */
    public void rollbackEvents() {
        eventBuffer.rollbackEvents();
    }

    /**
     * Forwards the call to the underlying event buffer. Also records the producer SCN on the metrics collector
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#endEvents(boolean, long, boolean, boolean, com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector)
     */
    public void endEvents(boolean b, long l, boolean b2, boolean b3, DbusEventsStatisticsCollector statisticsCollector) {
        eventBuffer.endEvents(b,l,b2,b3,statisticsCollector);
        collector.setProducerSCN(producerName,eventBuffer.lastWrittenScn());
    }

    /**
     * Forwards the call to the underlying event buffer. Also records the producer SCN on the metrics collector
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#endEvents(long, com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector)
     */
    public void endEvents(long l, DbusEventsStatisticsCollector statisticsCollector) {
        eventBuffer.endEvents(l, statisticsCollector);
        collector.setProducerSCN(producerName,eventBuffer.lastWrittenScn());
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#empty()
     */
    public boolean empty() {
        return eventBuffer.empty();
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#getMinScn()
     */
    public long getMinScn() {
        return eventBuffer.getMinScn();
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#lastWrittenScn()
     */
    public long lastWrittenScn() {
        return eventBuffer.lastWrittenScn();
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#setStartSCN(long)
     */
    public void setStartSCN(long l) {
        eventBuffer.setStartSCN(l);
    }

    /**
     * Forwards the call to the underlying event buffer
     * @see com.linkedin.databus.core.DbusEventBufferAppendable#getPrevScn()
     */
    public long getPrevScn() {
        return eventBuffer.getPrevScn();
    }

}
