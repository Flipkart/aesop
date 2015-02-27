package com.flipkart.aesop.runtime.producer;

import com.flipkart.aesop.runtime.metrics.MetricsCollector;
import com.linkedin.databus.core.*;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;

/**
 * @author kartikbu
 * @created 10/07/14
 */
public class ProducerEventBuffer implements DbusEventBufferAppendable {

    private String producerName;

    private DbusEventBufferAppendable eventBuffer;

    private MetricsCollector collector;

    public ProducerEventBuffer(String producerName, DbusEventBufferAppendable eventBuffer, MetricsCollector collector) {
        this.producerName = producerName;
        this.eventBuffer = eventBuffer;
        this.collector = collector;
    }

    @Override
    public void start(long l) {
        eventBuffer.start(l);
    }

    @Override
    public void startEvents() {
        eventBuffer.startEvents();
    }

    @Override
    public boolean appendEvent(DbusEventKey dbusEventKey, long l, short i, short i2, long l2, short i3, byte[] bytes, byte[] bytes2, boolean b, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey, l, i, i2, l2, i3, bytes, bytes2, b, statisticsCollector);
    }

    @Override
    public boolean appendEvent(DbusEventKey dbusEventKey, short i, short i2, long l, short i3, byte[] bytes, byte[] bytes2, boolean b) {
        return eventBuffer.appendEvent(dbusEventKey, i, i2, l, i3, bytes, bytes2, b);
    }

    @Override
    public boolean appendEvent(DbusEventKey dbusEventKey, short i, short i2, long l, short i3, byte[] bytes, byte[] bytes2, boolean b, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey,i,i2,l,i3,bytes,bytes2,b,statisticsCollector);
    }

    @Override
    public boolean appendEvent(DbusEventKey dbusEventKey, short i, short i2, long l, short i3, byte[] bytes, byte[] bytes2, boolean b, boolean b2, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey, i, i2, l, i3, bytes, bytes2, b, b2, statisticsCollector);
    }

    @Override
    public boolean appendEvent(DbusEventKey dbusEventKey, DbusEventInfo dbusEventInfo, DbusEventsStatisticsCollector statisticsCollector) {
        return eventBuffer.appendEvent(dbusEventKey, dbusEventInfo, statisticsCollector);
    }

    @Override
    public void rollbackEvents() {
        eventBuffer.rollbackEvents();
    }

    @Override
    public void endEvents(boolean b, long l, boolean b2, boolean b3, DbusEventsStatisticsCollector statisticsCollector) {
        eventBuffer.endEvents(b,l,b2,b3,statisticsCollector);
        collector.setProducerSCN(producerName,eventBuffer.lastWrittenScn());
    }

    @Override
    public void endEvents(long l, DbusEventsStatisticsCollector statisticsCollector) {
        eventBuffer.endEvents(l, statisticsCollector);
        collector.setProducerSCN(producerName,eventBuffer.lastWrittenScn());
    }

    @Override
    public boolean empty() {
        return eventBuffer.empty();
    }

    @Override
    public long getMinScn() {
        return eventBuffer.getMinScn();
    }

    @Override
    public long lastWrittenScn() {
        return eventBuffer.lastWrittenScn();
    }

    @Override
    public void setStartSCN(long l) {
        eventBuffer.setStartSCN(l);
    }

    @Override
    public long getPrevScn() {
        return eventBuffer.getPrevScn();
    }

}
