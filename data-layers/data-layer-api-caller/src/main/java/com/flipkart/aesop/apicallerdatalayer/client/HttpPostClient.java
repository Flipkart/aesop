package com.flipkart.aesop.apicallerdatalayer.client;


import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Created by aman.gupta on 09/12/15.
 */
public interface HttpPostClient {
    //Request request = new Request(url,payload,headers);
    //Response response = client.executePost(request);

    public Response post(String url, String payload, Map<String,String> headers);
}
