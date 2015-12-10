package com.flipkart.aesop.apicallerdatalayer.headers;

import com.google.common.base.Splitter;

import java.util.Map;

/**
 * Created by aman.gupta on 10/12/15.
 */
public class HeaderCreater {
    Map<String,String> headers;

    public HeaderCreater(String headers) {
        this.headers = Splitter.on(",").withKeyValueSeparator("=").split(headers);
    }
}
