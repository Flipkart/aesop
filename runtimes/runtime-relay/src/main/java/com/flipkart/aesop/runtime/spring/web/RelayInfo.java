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
	/** End Getter/Setter methods*/

	public static class ClientInfo {
		private String clientName;
		private String clientSinceSCN;
		/** Constructor with only client name */
		public ClientInfo(String clientName) {
			this.clientName = clientName;
		}				
		/** Getter/Setter methods*/
		public String getClientName() {
			return clientName;
		}		
		public void setClientSinceSCN(String clientSinceSCN) {
			this.clientSinceSCN = clientSinceSCN;
		}
		public String getClientSinceSCN() {
			return clientSinceSCN;
		}
		/** End Getter/Setter methods*/
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
