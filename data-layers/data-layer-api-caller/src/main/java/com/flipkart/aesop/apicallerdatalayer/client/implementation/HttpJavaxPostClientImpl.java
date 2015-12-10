package com.flipkart.aesop.apicallerdatalayer.client.implementation;


import com.flipkart.aesop.apicallerdatalayer.ApiCallerDataLayer;
import com.flipkart.aesop.apicallerdatalayer.client.HttpPostClient;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Created by aman.gupta on 10/12/15.
 */
public class HttpJavaxPostClientImpl implements HttpPostClient {
    Client client = ClientBuilder.newClient();
    private static final Logger LOGGER = LogFactory.getLogger(ApiCallerDataLayer.class);
    @Override
    public Response post(String url, String payload, Map<String, String> headers) {

        Response response = client.target(url).request().post(Entity.entity(payload, MediaType.APPLICATION_JSON_TYPE));
        LOGGER.info(response.readEntity(String.class));
        return response;
    }
}
