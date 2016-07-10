package com.flipkart.aesop.apicallerdatalayer.headers.impl;

import com.flipkart.aesop.apicallerdatalayer.headers.StaticHeaderProvider;
import com.google.common.base.Splitter;

import java.util.Map;

/**
 * Created by aman.gupta on 14/12/15.
 */
public class StringBasedStaticHeaderProviderImpl implements StaticHeaderProvider {
    Map<String,String> headers;

    public StringBasedStaticHeaderProviderImpl(String headers) {
        this.headers = Splitter.on(",").withKeyValueSeparator("=").split(headers);
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }
}
