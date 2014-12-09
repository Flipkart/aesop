package com.flipkart.aesop.elasticsearchdatalayer.config;

/**
 * Passes ElasticSearchConfig filename
 * @author Pratyay Banerjee
 */

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

public class ElasticSearchConfig implements  InitializingBean{
    public String config;

    public ElasticSearchConfig(String config)
    {
        this.config=config;
    }
    public void setConfig(String config){
        this.config  = config;
    }
    public String getConfig(){
        return this.config;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        //Assert if filename is ot empty
        Assert.notNull(this.config,"'ElasticSearchConfig' cannot be null. This Databus Client will not be initialized");
    }
}
