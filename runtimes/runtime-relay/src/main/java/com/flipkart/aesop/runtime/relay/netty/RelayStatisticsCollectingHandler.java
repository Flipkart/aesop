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
import com.linkedin.databus.container.request.ReadEventsRequestProcessor;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.container.monitoring.mbean.DbusHttpTotalStats;
import com.linkedin.databus2.core.container.monitoring.mbean.HttpStatisticsCollector;
import com.linkedin.databus2.core.container.request.DatabusRequest;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.net.InetSocketAddress;

/**
 * The <code>RelayStatisticsCollectingHandler</code> class is code port of the Databus {@link com.linkedin.databus.container.netty.RelayStatisticsCollectingHandler} that
 * listens for DatabusRequest messages sent over the pipeline and assigns connection-specific relay stats collectors.
 * Additionally, this collector also registers the stream request with connection-specific {@link DbusHttpTotalStats} if the Databus request is for reading events 
 * i.e. contains a {@link Checkpoint}
 * 
 * @author Regunath B
 * @version 1.0, 12 May 2014
 */
public class RelayStatisticsCollectingHandler extends SimpleChannelHandler {

	/** Logger for this class*/
	protected static final Logger LOGGER = LogFactory.getLogger(RelayStatisticsCollectingHandler.class);

	/** Member variables for this class*/
	private DbusEventsStatisticsCollector outEventStatsCollector;
	private DbusEventsStatisticsCollector connOutEventStatsCollector;
	private HttpStatisticsCollector outHttpStatsCollector;
	private HttpStatisticsCollector connOutHttpStatsCollector;
	private DatabusRequest latestDbusRequest = null;

    /** relay instance */
    private DefaultRelay relay;
    
    /** the client reference*/
    private String client = null;

	public RelayStatisticsCollectingHandler(DefaultRelay relay) {
        this.relay = relay;
	    outEventStatsCollector = relay.getOutboundEventStatisticsCollector();
	    outHttpStatsCollector = relay.getHttpStatisticsCollector();
	    connOutEventStatsCollector = null;	    
	}
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
		if (null != outEventStatsCollector || null != outHttpStatsCollector) {
			//Opening a new connection
			Object value = e.getValue();
			if (value instanceof InetSocketAddress){
				InetSocketAddress inetAddress = (InetSocketAddress)value;
				this.client = inetAddress.getAddress().isLoopbackAddress() ?
						"localhost" : inetAddress.getAddress().getHostAddress();
				this.client = this.client + "-" + inetAddress.getPort();
			} else {
				this.client = e.getValue().toString();
			}
			if (null != outEventStatsCollector){
				connOutEventStatsCollector = outEventStatsCollector.createForPeerConnection(client);
			} if (null != outHttpStatsCollector) {
				connOutHttpStatsCollector = outHttpStatsCollector.createForClientConnection(client);
			}
		}
		// Inform the Relay of a client connection, for stats tracking
		this.relay.firePeerConnect(this.client);		
		super.channelConnected(ctx, e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		try
        {
	        if (null != outEventStatsCollector || null != outHttpStatsCollector )
	        {
	        	if (e.getMessage() instanceof DatabusRequest){
	        		latestDbusRequest = (DatabusRequest)e.getMessage();
	        		if (null != outEventStatsCollector){
	        			latestDbusRequest.getParams().put(outEventStatsCollector.getName(),
	        					connOutEventStatsCollector);
	        		}
	        		if (null != outHttpStatsCollector){
	        			latestDbusRequest.getParams().put(outHttpStatsCollector.getName(),
	        					connOutHttpStatsCollector);
	        		}
	        		// check if a checkpoint exists and register the stream request with the connection-specific stats
	        		if (latestDbusRequest.getParams().getProperty(ReadEventsRequestProcessor.CHECKPOINT_PARAM) != null) {
	        			Checkpoint cp = new Checkpoint(latestDbusRequest.getParams().getProperty(ReadEventsRequestProcessor.CHECKPOINT_PARAM));
	        			String peer = connOutHttpStatsCollector.getPeers().get(0); // the only peer that the connection-specific stats collector will have points to the remote client
	        			connOutHttpStatsCollector.getPeerStats(peer).registerStreamRequest(peer, cp);
                        relay.getMetricsCollector().setClientSCN(peer,cp.getWindowScn());
	        		}
                    // update client scn
	        	} else if (shouldMerge(e)){
	        		//First or Last message in a call
	        		mergePerConnStats();
	        	}
	        }
	        super.messageReceived(ctx, e);
        }
        catch (Exception ex)
        {
        	LOGGER.error("Exception while processing message in RelayStatisticsCollectingHandler");
        }
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception{
		if (null != connOutEventStatsCollector || null != connOutHttpStatsCollector){
			mergePerConnStats();
			if (null != connOutEventStatsCollector) connOutEventStatsCollector.unregisterMBeans();
			if (null != connOutHttpStatsCollector) connOutHttpStatsCollector.unregisterMBeans();
			connOutEventStatsCollector = null;
			connOutHttpStatsCollector = null;
		}
		latestDbusRequest = null;
		// Inform the Relay of a client disconnect, for stats tracking
		this.relay.firePeerDisconnect(this.client);
		super.channelClosed(ctx, e);
	}

	private boolean shouldMerge(MessageEvent me){
		return ((me.getMessage() instanceof HttpChunkTrailer) ||
				(me.getMessage() instanceof HttpResponse));
	}

	private void mergePerConnStats(){
		if (null != connOutEventStatsCollector){
			outEventStatsCollector.merge(connOutEventStatsCollector);
			connOutEventStatsCollector.reset();
		}
		if (null != connOutHttpStatsCollector){
			outHttpStatsCollector.merge(connOutHttpStatsCollector);
			connOutHttpStatsCollector.reset();
		}
	}

}
