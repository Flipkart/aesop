package com.flipkart.aesop.apicallerdatalayer;

import com.flipkart.aesop.apicallerdatalayer.client.HttpPostClient;
import com.flipkart.aesop.apicallerdatalayer.headers.ContextualHeaderProvider;
import com.flipkart.aesop.apicallerdatalayer.headers.StaticHeaderProvider;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.processor.DestinationEventProcessor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import javax.naming.OperationNotSupportedException;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by aman.gupta on 08/12/15.
 */
public class ApiCallerDataLayer implements DestinationEventProcessor{
    private String url;
    private StaticHeaderProvider staticHeaderProvider;
    private ContextualHeaderProvider contextualHeaderProvider;
    private HttpPostClient httpPostClient;
    private static final Logger LOGGER = LogFactory.getLogger(ApiCallerDataLayer.class);

    public void setStaticHeaderProvider(StaticHeaderProvider staticHeaderProvider) {
        this.staticHeaderProvider = staticHeaderProvider;
    }

    public void setContextualHeaderProvider(ContextualHeaderProvider contextualHeaderProvider) {
        this.contextualHeaderProvider = contextualHeaderProvider;
    }

    public void setHttpPostClient(HttpPostClient httpPostClient) {
        this.httpPostClient = httpPostClient;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    @Override
    public ConsumerCallbackResult processDestinationEvent(AbstractEvent destinationEvent) throws OperationNotSupportedException {
        try {
            Map<String, Object> eventMap = destinationEvent.getFieldMapPair();
            Gson gson = new GsonBuilder().serializeNulls().create();
            String payload = gson.toJson(eventMap);
            Map<String, String> headers = new HashMap<String, String>();
            headers.putAll(staticHeaderProvider.getHeaders());
            headers.putAll(contextualHeaderProvider.getHeaders(eventMap));
            headers.put("Content-Type", "application/json");
            LOGGER.info("Making a post call to url: " + url + " with payload as: " + payload + " and headers as: " + headers);
            Response response = httpPostClient.post(url, payload, headers);
            int responseCode = response.getStatus();
            if (responseCode >= 200 && responseCode < 300) {
                LOGGER.info("Call successful with response code as " + responseCode + " for payload: " + payload);
                return ConsumerCallbackResult.SUCCESS;
            } else {
                LOGGER.info("Call unsuccessful with response code as " + responseCode + " and message as " + response.readEntity(String.class) + " for payload: " + payload);
                return ConsumerCallbackResult.ERROR;
            }
        }catch(Exception e){
            LOGGER.error("Call unsuccessful with error: ",e);
            return ConsumerCallbackResult.ERROR;
        }
    }


}
