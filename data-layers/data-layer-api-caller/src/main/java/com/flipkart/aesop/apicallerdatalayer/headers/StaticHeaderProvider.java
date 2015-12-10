package com.flipkart.aesop.apicallerdatalayer.headers;

import java.util.Map;

/**
 * Created by aman.gupta on 10/12/15.
 */
public class StaticHeaderProvider extends HeaderCreater {
    public StaticHeaderProvider(String headers) {
        super(headers);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

}
