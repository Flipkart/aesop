package com.flipkart.aesop.apicallerdatalayer.headers;

import java.util.Map;

/**
 * Created by aman.gupta on 14/12/15.
 */
public interface ContextualHeaderProvider {
    public Map<String, String> getHeaders(Map<String, Object> event);
}
