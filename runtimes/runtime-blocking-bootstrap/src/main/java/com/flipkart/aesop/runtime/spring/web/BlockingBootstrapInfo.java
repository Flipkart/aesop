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

import java.util.Map;

/**
 * <code>BlockingBootstrapInfo</code> holds information for rendering Relay details.
 *
 * @version 1.0, 12 May 2014
 */
public class BlockingBootstrapInfo
{
	
	/** Member variables holding display attributes*/
	private int pSourceId;
	private String pSourceName;
	private String pSourceURI;

	private String producerName;
	private String producerSinceSCN;

    private LSourceInfo[] lSourceInfos;
    private Map<String,Map<String, Long>> hostGroupedClient;

	/** Constructor with only physical source attributes*/
	public BlockingBootstrapInfo(int pSourceId, String pSourceName, String pSourceURI) {
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
    public LSourceInfo[] getlSourceInfos() {
        return lSourceInfos;
    }
    public void setlSourceInfos(LSourceInfo[] lSourceInfos) {
        this.lSourceInfos = lSourceInfos;
    }

	/** End Getter/Setter methods*/
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
