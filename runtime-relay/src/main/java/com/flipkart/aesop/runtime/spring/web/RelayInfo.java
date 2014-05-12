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
	
	private int lSourceId;
	private String lSourceName;
	private String lSourceURI;
	
	private String producerName;
	private String producerSinceSCN;
	
	/** Constructor with only physical source attributes*/
	public RelayInfo(int pSourceId, String pSourceName, String pSourceURI) {
		super();
		this.pSourceId = pSourceId;
		this.pSourceName = pSourceName;
		this.pSourceURI = pSourceURI;
	}

	/** Getter/Setter methods*/
	public int getlSourceId() {
		return lSourceId;
	}
	public void setlSourceId(int lSourceId) {
		this.lSourceId = lSourceId;
	}
	public String getlSourceName() {
		return lSourceName;
	}
	public void setlSourceName(String lSourceName) {
		this.lSourceName = lSourceName;
	}
	public String getlSourceURI() {
		return lSourceURI;
	}
	public void setlSourceURI(String lSourceURI) {
		this.lSourceURI = lSourceURI;
	}
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
	
}
