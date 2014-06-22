package com.flipkart.aesop.runtime.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsTotalStats;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author kartikbu
 * @created 09/06/14
 */
public class MetricsCollector {

    /** Default refresh interval */
    private static final int DEFAULT_REFRESH_INTERVAL = 3;

    /** Number of data points in history */
    private static final int DEFAULT_MAX_SIZE = 60;

    /** Scheduler for doing the encoding */
    private ScheduledExecutorService scheduledExecutorService;

    /** Object mapper */
    private ObjectMapper objectMapper = new ObjectMapper();

    /** JSON encoded registry */
    private String json = "";

    /** refresh interval */
    private int refreshInterval = DEFAULT_REFRESH_INTERVAL;

    /** Stats Collectors */
    private DbusHttpTotalStats httpTotalStats;
    private DbusEventsTotalStats inboundTotalStats;
    private DbusEventsTotalStats outboundTotalStats;

    public MetricsCollector(DbusHttpTotalStats httpTotalStats, DbusEventsTotalStats inboundStats, DbusEventsTotalStats outboundStats) {

        // stats collectors
        this.httpTotalStats = httpTotalStats;
        this.inboundTotalStats = inboundStats;
        this.outboundTotalStats = outboundStats;

        // schedule encoder thread
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Encoder(this), 0, DEFAULT_REFRESH_INTERVAL, TimeUnit.SECONDS);

    }


    public String getJson() {
        return json;
    }

    public int getRefreshInterval() {
        return refreshInterval;
    }

    /**
     * Encoder class which encodes (JSON) registry intsance.
     */
    public class Encoder implements Runnable {

        private MetricsCollector collector;

        public Encoder(MetricsCollector collector) {
            this.collector = collector;
        }

        /**
         * Interface method implementation.
         */
        @Override
        public void run() {
            Map<String,Object> map = new HashMap<String,Object>();
            map.put("http",this.collector.httpTotalStats);
            map.put("inbound",this.collector.inboundTotalStats);
            map.put("outbound",this.collector.outboundTotalStats);
            try {
                this.collector.json = this.collector.objectMapper.writeValueAsString(map);
            } catch (Exception e) {
                this.collector.json = this.mapException(e);
            }
        }

        /**
         * In case of exceptions while encoding this method is used to send an alternate JSON.
         * @param e     Exception caught
         * @return      String representation of exception.
         */
        private String mapException(Exception e) {
            try {
                Map<String,String> map = new HashMap<String,String>();
                map.put("status", "exception");
                map.put("class", e.getClass().getName());
                map.put("message", e.getMessage());
                return objectMapper.writeValueAsString(map);
            } catch (Exception x) {
                return "";
            }
        }

    }


}
