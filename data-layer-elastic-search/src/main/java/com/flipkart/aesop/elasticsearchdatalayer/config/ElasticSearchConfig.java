package com.flipkart.aesop.elasticsearchdatalayer.config;

/**
 * Created with IntelliJ IDEA.
 * User: pratyay.banerjee
 * Date: 05/12/14
 * Time: 12:16 PM
 * To change this template use File | Settings | File Templates.
 */
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.runtime.common.RuntimeVariables;

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
        //To change body of implemented methods use File | Settings | File Templates.
        Assert.notNull(this.config,"'ElasticSearchConfig' cannot be null. This Databus Client will not be initialized");
    }
}
