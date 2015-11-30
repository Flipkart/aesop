package com.flipkart.aesop.apicallerdatalayer.upsert;

import com.flipkart.aesop.apicallerdatalayer.delete.ApiCallerDeleteDataLayer;
import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.json.JSONException;
import org.json.JSONObject;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;

/**
 * Created by aman.gupta on 12/08/15.
 */
public class ApiCallerUpsertDataLayer extends UpsertDestinationStoreProcessor
{
    private static final Logger LOGGER = LogFactory.getLogger(ApiCallerUpsertDataLayer.class);
    private URL url;
    private JSONObject headers;
    public ApiCallerUpsertDataLayer(URL url, JSONObject headers) {
        this.url = url;
        this.headers = headers;
    }

    @Override
    protected ConsumerCallbackResult upsert(AbstractEvent event) {
        try {
            JSONObject param = new JSONObject(event.getFieldMapPair());
            final String USER_AGENT = "Mozilla/5.0";
            LOGGER.info("Making a post call to url: "+url+" with payload as: "+param.toString());
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", USER_AGENT);
            con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            Iterator<String> it = headers.keys();
            //If a header value starts with $, the corresponding value from the payload is used.
            while(it.hasNext()){
                String header = it.next();
                String value = headers.get(header).toString();
                if(value.charAt(0)=='$'){
                    value = param.get(value.substring(1)).toString();
                }
                con.setRequestProperty(header,value);
            }
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(param.toString());
            wr.flush();
            wr.close();
            int responseCode = con.getResponseCode();
            LOGGER.info("Call successful with response code as "+responseCode+ "for payload: "+param.toString());
            return ConsumerCallbackResult.SUCCESS;
        } catch(Exception e){
            LOGGER.error("Call unsuccessful with error:",e);
            return ConsumerCallbackResult.ERROR;
        }
    }
}