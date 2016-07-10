package com.flipkart.aesop.apicallerdatalayer.client.implementation;

import com.flipkart.aesop.apicallerdatalayer.client.HttpPostClient;
import com.flipkart.casclient.client.HttpAuthClient;
import com.flipkart.casclient.entity.Request;
import flipkart.platform.cachefarm.Cache;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;

/**
 * Created by aman.gupta on 09/12/15.
 */
public class HttpCASPostClientImpl implements HttpPostClient {
    private HttpAuthClient httpAuthClient;
    private static final Logger LOGGER = LogFactory.getLogger(HttpCASPostClientImpl.class);
    public HttpCASPostClientImpl(String casUrl, String user, String password, boolean enableAuth, Cache cache) {
        this.httpAuthClient = new HttpAuthClient(casUrl,user,password,enableAuth,cache);
    }

    @Override
    public Response post(String url, String payload, Map<String, String> headers) {
        Request request = new Request(url,payload,headers);
        com.ning.http.client.Response response = httpAuthClient.executePost(request);
        String responseBody;
        try {
            responseBody = response.getResponseBody();
        } catch (IOException e) {
            LOGGER.error("Unable to get response body for payload"+payload+" Keeping it blank.",e);
            responseBody = "";
        }
        return Response.status(response.getStatusCode()).entity(responseBody).build();
    }
}
