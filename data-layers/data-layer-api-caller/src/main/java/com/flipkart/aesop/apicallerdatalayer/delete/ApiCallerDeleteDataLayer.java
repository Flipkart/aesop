package com.flipkart.aesop.apicallerdatalayer.delete;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.event.AbstractEvent;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import org.json.JSONException;
import org.json.JSONObject;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Iterator;

/**
 * Created by aman.gupta on 12/08/15.
 */
public class ApiCallerDeleteDataLayer extends DeleteDestinationStoreProcessor {

    private static final Logger LOGGER = LogFactory.getLogger(ApiCallerDeleteDataLayer.class);
    private URL url;
    private JSONObject headers;
    public ApiCallerDeleteDataLayer(URL url, JSONObject headers) {
        this.url = url;
        this.headers = headers;
    }

    @Override
    protected  ConsumerCallbackResult delete(AbstractEvent event) {
        try {
            /*
            JSONObject param =new JSONObject();
            Object[] keyset = event.getFieldMapPair().keySet().toArray();
            Object[] values = event.getFieldMapPair().values().toArray();
            for(int i=0;i<event.getFieldMapPair().size();i++)
            {
                param.put(String.valueOf(keyset[i]),String.valueOf(values[i]));
            }
            */
            JSONObject param = new JSONObject(event.getFieldMapPair());
            final String USER_AGENT = "Mozilla/5.0";
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", USER_AGENT);
            con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            Iterator<String> it = headers.keys();
            while(it.hasNext()){
                String header = it.next();
                String value = headers.get(header).toString();
                if(value.charAt(0)=='$'){
                    value = param.get(value.substring(1)).toString();
                }
                con.setRequestProperty(header,value);
            }
            // Send post request
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(param.toString());
            LOGGER.info(param.toString());
            wr.flush();
            wr.close();
            int responseCode = con.getResponseCode();
            switch (responseCode) {
                case 200:
                    LOGGER.info("API called successfully");
                    return ConsumerCallbackResult.SUCCESS;
                case 201:
                    LOGGER.info("API called successfully");
                    return ConsumerCallbackResult.SUCCESS;
                case 202:
                    LOGGER.info("API called successfully");
                    return ConsumerCallbackResult.SUCCESS;
                case 204:
                    LOGGER.info("API called successfully");
                    return ConsumerCallbackResult.SUCCESS;
                default:
                    LOGGER.info("API COULD NOT BE CALLED!! Response Code:"+responseCode);
                    return ConsumerCallbackResult.ERROR;
            }
        }
        catch (ProtocolException e) {
            e.printStackTrace();
            LOGGER.info("API COULD NOT BE CALLED!! ProtocolException Occurred");
        }
        catch (IOException e) {
            e.printStackTrace();
            LOGGER.info("API COULD NOT BE CALLED!! IOException Occurred");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        LOGGER.info("API COULD NOT BE CALLED!! Some shit happened");
        return ConsumerCallbackResult.ERROR;
    }
}