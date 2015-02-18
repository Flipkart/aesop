/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *******************************************************************************/
package com.flipkart.aesop.processor.es.config;

/**
 * Passes ElasticSearchConfig filename
 * @author Pratyay Banerjee
 */

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

public class ElasticSearchConfig implements  InitializingBean
{
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

    public void afterPropertiesSet() throws Exception {
        /* Assert if filename is ot empty */
        Assert.notNull(this.config,"'ElasticSearchConfig' cannot be null. This Databus Client will not be initialized");
    }
}
