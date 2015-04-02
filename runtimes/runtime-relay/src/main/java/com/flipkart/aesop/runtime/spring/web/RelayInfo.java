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
package com.flipkart.aesop.runtime.spring.web;

import java.util.HashMap;
import java.util.Map;

/**
 * <code>RelayInfo</code> holds information for rendering Relay details.
 *
 * @author Regunath B
 * @version 1.0, 12 May 2014
 */
public class RelayInfo {
	
	/** Member variables holding display attributes*/
	private int pSourceId;
	private String pSourceName;
	private String pSourceURI;

	private String producerName;
	private String producerSinceSCN;

	private RelayInfo.ClientInfo[] clientInfos;
    private RelayInfo.LSourceInfo[] lSourceInfos;
    private Map<String,Map<String, Long>> hostGroupedClient;
	
	/** Constructor with only physical source attributes*/
	public RelayInfo(int pSourceId, String pSourceName, String pSourceURI) {
		super();
		this.pSourceId = pSourceId;
		this.pSourceName = pSourceName;
		this.pSourceURI = pSourceURI;
	}

	/** Getter/Setter methods*/

	public String getProducerName() {
		return producerName;
	}
	public void setProducerName(String producerName) {
		this.producerName = producerName;
	}
	public String getProducerSinceSCN() {
		return producerSinceSCN;
	}
	public void setProducerSinceSCN(String producerSinceSCN) {
		this.producerSinceSCN = producerSinceSCN;
	}
	public int getpSourceId() {
		return pSourceId;
	}
	public String getpSourceName() {
		return pSourceName;
	}
	public String getpSourceURI() {
		return pSourceURI;
	}
	public RelayInfo.ClientInfo[] getClientInfos() {
		return clientInfos;
	}
	public void setClientInfos(RelayInfo.ClientInfo[] clientInfos) {
		this.clientInfos = clientInfos;
	}
    public RelayInfo.LSourceInfo[] getlSourceInfos() {
        return lSourceInfos;
    }
    public void setlSourceInfos(RelayInfo.LSourceInfo[] lSourceInfos) {
        this.lSourceInfos = lSourceInfos;
    }

    public Map<String,Map<String, Long>> getHostGroupedClient() { return this.hostGroupedClient; }
    public void setHostGroupedClient() {
        this.hostGroupedClient = new HashMap<String, Map<String, Long>>();

        Long clientSCN;
        String clientHost;
        for(RelayInfo.ClientInfo clientInfo : this.clientInfos) {

            clientSCN = clientInfo.getClientSinceSCN();

            clientHost = clientInfo.getClientHost();

            Map<String, Long> clientHostSCN = this.hostGroupedClient.get(clientHost);

            if(clientHostSCN == null) {
                clientHostSCN = new HashMap<String, Long>();
            }

            if(clientHostSCN.get("MIN") == null || clientHostSCN.get("MIN") > clientSCN) {
                clientHostSCN.put("MIN", clientSCN);
            }

            if(clientHostSCN.get("MAX") == null || clientHostSCN.get("MAX") < clientSCN) {
                clientHostSCN.put("MAX", clientSCN);
            }

            this.hostGroupedClient.put(
                    clientHost, clientHostSCN
            );
        }
    }
	/** End Getter/Setter methods*/

    public static class ClientInfo {
		private String clientName;
        private String clientHost;
		private Long   clientSinceSCN;

        /** Constructor with only client name */
		public ClientInfo(String clientName) {
			this.clientName = clientName;
            // remove the partition id from the client name to retrieve the host
            this.clientHost = ClientInfo.parseHostFromClientName(this.clientName);
		}

		/** Getter/Setter methods*/
		public String getClientName() { return this.clientName; }

        public String getClientHost() { return this.clientHost; }

		public void setClientSinceSCN(Long clientSinceSCN) {
			this.clientSinceSCN = clientSinceSCN;
		}

		public Long getClientSinceSCN() { return clientSinceSCN; }
		/** End Getter/Setter methods*/

        /**
         * ClientName is of the format 'host-port'. Remove the port
         * @param clientName
         * @return
         */
        public static String parseHostFromClientName(String clientName) {
            return clientName.replaceAll("(.*)-(\\d+)", "$1");
        }
	}

    public static class LSourceInfo {
        private int lSourceId;
        private String lSourceName;
        private String lSourceURI;

        /** Constructor with only lSourceId name */
        public LSourceInfo(int lSourceId) {
            this.lSourceId = lSourceId;
        }

        /** Getter/Setter methods*/
        public int getLSourceId() {
            return lSourceId;
        }
        public void setLSourceId(int lSourceId) {
            this.lSourceId = lSourceId;
        }
        public String getLSourceName() {
            return lSourceName;
        }
        public void setLSourceName(String lSourceName) {
            this.lSourceName = lSourceName;
        }
        public String getLSourceURI() {
            return lSourceURI;
        }
        public void setLSourceURI(String lSourceURI) {
            this.lSourceURI = lSourceURI;
        }
        /** End Getter/Setter methods*/
    }

}
