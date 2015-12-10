package com.flipkart.aesop.apicallerdatalayer.headers;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by aman.gupta on 10/12/15.
 */
public class ContextualHeaderProvider extends HeaderCreater {
    public ContextualHeaderProvider(String headers) {
        super(headers);
    }

    public Map<String, String> getHeaders(Map<String, Object> event) {
        Map<String,String> map = new HashMap<String,String>();
        for(Map.Entry<String,String> entry : headers.entrySet()){
            map.put(entry.getKey(),(String)event.get(entry.getValue()));
        }
        return map;
    }

}
