package com.flipkart.aesop.apicallerdatalayer.headers.impl;

import com.flipkart.aesop.apicallerdatalayer.headers.ContextualHeaderProvider;
import com.google.common.base.Splitter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by aman.gupta on 14/12/15.
 */
public class StringBasedContextualHeaderProviderImpl implements ContextualHeaderProvider {
    Map<String,String> headers;

    public StringBasedContextualHeaderProviderImpl(String headers) {
        this.headers = Splitter.on(",").withKeyValueSeparator("=").split(headers);
    }

    @Override
    public Map<String, String> getHeaders(Map<String, Object> event) {
        Map<String,String> map = new HashMap<String,String>();
        for(Map.Entry<String,String> entry : headers.entrySet()){
            map.put(entry.getKey(),(String)event.get(entry.getValue()));
        }
        return map;
    }
}
