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
package com.flipkart.aesop.runtime.relay.netty;

import com.flipkart.aesop.runtime.relay.DefaultRelay;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

/**
 * The <code>HttpRelayPipelineFactory</code> class is a code port of the Databus {@link com.linkedin.databus.container.netty.HttpRelayPipelineFactory} that 
 * registers the Aesop RelayStatisticsCollectingHandler instead of the Databus {@link com.linkedin.databus.container.netty.RelayStatisticsCollectingHandler}
 * 
 * @author Regunath B
 * @version 1.0, 12 May 2014
 */
public class HttpRelayPipelineFactory implements ChannelPipelineFactory {

	/** Member variables for this class */
	private final DefaultRelay relay;
	private ChannelPipelineFactory oldPipelineFactory;
	
	public HttpRelayPipelineFactory(DefaultRelay relay, ChannelPipelineFactory oldPipelineFactory) {
		super();
		this.relay = relay;
		this.oldPipelineFactory = oldPipelineFactory;
	}
	
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = oldPipelineFactory.getPipeline();
        RelayStatisticsCollectingHandler relayStatsHandler =
            new RelayStatisticsCollectingHandler(relay);
        pipeline.addBefore("databusRequestRunner", "relayStatsCollector", relayStatsHandler);        
        return pipeline;
    }
}