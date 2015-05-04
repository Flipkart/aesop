/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.aesop.runtime.bootstrap.producer.registeration;

import org.springframework.util.Assert;

/**
 * <code>InitBackedProducerRegistration</code> holds information with init Scn properties 
 * for registering a Databus {@link com.linkedin.databus2.producers.EventProducer} with the
 * {@link com.flipkart.aesop.runtime.bootstrap.BlockingBootstrapServer} against a {@link com.linkedin.databus2.relay.config.PhysicalSourceConfig}
 *
 * @author aryaKetan
 * @version 1.0, 02 April 2015
 */
public class InitBackedProducerRegistration extends ProducerRegistration {

    /**
     * Interface method implementation. Ensures that a EventProducer and PhysicalSourceConfig is set
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Assert.notNull(this.getProperties(),
                "'properties' cannot be null, It must be specified");
         Assert.notNull(this.getProperties().get("databus.bootstrap.dataSources.sequenceNumbersHandler.file.initVal"),
                "'properties#initScn' cannot be null. A initScn as property key databus.relay.dataSources.sequenceNumbersHandler.file.initVal must be specified");
    }
}
