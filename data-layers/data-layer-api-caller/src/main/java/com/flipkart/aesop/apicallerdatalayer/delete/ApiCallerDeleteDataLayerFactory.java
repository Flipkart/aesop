package com.flipkart.aesop.apicallerdatalayer.delete;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.FactoryBean;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by aman.gupta on 12/08/15.
 */
public class ApiCallerDeleteDataLayerFactory implements FactoryBean<ApiCallerDeleteDataLayer> {
    private URL url;
    private JSONObject headers;
    public ApiCallerDeleteDataLayer getObject() throws Exception
    {
        return new ApiCallerDeleteDataLayer(url,headers);
    }

    public Class<?> getObjectType()
    {
        return ApiCallerDeleteDataLayer.class;
    }

    public boolean isSingleton()
    {
        return true;
    }

    public void setUrl(String url) {
        try {
            this.url = new URL(url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }
    public void setHeaders(String headers){
        try {
            this.headers=new JSONObject(headers);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}