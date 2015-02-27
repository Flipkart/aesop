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
package com.flipkart.aesop.serializer.batch.reader;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.serializer.model.UserInfo;

/**
 * The <code>UserInfoServiceScanReader</code> class is an implementation of the {@link ItemStreamReader} that invokes JSON over REST calls to a deployed service 
 * for scanning and retrieving the entire set of sample data item {@link UserInfo} instances
 * 
 * @author Regunath B
 * @version 1.0, 25 Mar 2014
 */

public class UserInfoServiceScanReader <T extends UserInfo> implements ItemStreamReader<UserInfo> {

	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(UserInfoServiceScanReader.class);
	
	/** The max results count*/
	private static final int MAX_RESULTS = 10000;	
	
	/** The service endpoint URL. Note: This is for testing and very specific to this sample*/
	private static final String BATCH_SERVICE_URL = "http://localhost:25151/userservice/v0.1/customer/batch";
	
	/** The batch size*/
	private static final int BATCH_SIZE = 500;
	
	/** The ObjectMapper to use for JSON deserialization*/
	private ObjectMapper objectMapper = new ObjectMapper();
	
	/** The result counter*/
	private int resultCount = (0 - BATCH_SIZE);
	
	/** The Semaphore to control concurrency */
	private Semaphore parallelFetch = new Semaphore(5);
	
	/** The local list containing data items*/
	private Queue<UserInfo> localQueue = new ConcurrentLinkedQueue<UserInfo>(); 	
	
	/**
	 * Interface method implementation.Scans and retrieves a number of {@link UserInfo} instances looked up from a service end-point. The scan stops once no more 
	 * data is returned by the service endpoint.
	 * Note : The end-point and parameters used here are very specific to this sample. Also the code is mostly for testing and production 
	 * ready (no Http connection pools etc.) 
	 * @see org.springframework.batch.item.ItemReader#read()
	 */
	public UserInfo read() throws Exception, UnexpectedInputException, ParseException  {
		// return data from local queue if available already
		synchronized(this) { // include the check for empty and remove in one synchronized block to avoid race conditions
			if (!this.localQueue.isEmpty()) {
				LOGGER.debug("Returning data from local cache. Cache size : " + this.localQueue.size());
				return this.localQueue.poll();
			}		
		}
		parallelFetch.acquire();
		int startIndex = 0;
		synchronized(this) {
			startIndex = resultCount += BATCH_SIZE;
		}
		if (this.resultCount < MAX_RESULTS) { 
			DefaultHttpClient httpclient  =  new DefaultHttpClient();
			HttpGet executionGet= new HttpGet(BATCH_SERVICE_URL);
			URIBuilder uriBuilder = new URIBuilder(executionGet.getURI());
			uriBuilder.addParameter("start",String.valueOf(startIndex));
			uriBuilder.addParameter("count",String.valueOf(BATCH_SIZE));
			((HttpRequestBase) executionGet).setURI(uriBuilder.build());
	        HttpResponse httpResponse = httpclient.execute(executionGet);
			String response = new String(EntityUtils.toByteArray(httpResponse.getEntity()));
			ScanResult scanResult = objectMapper.readValue(response, ScanResult.class);
			if (scanResult.getCount() <= 0) {	
				parallelFetch.release();
				return null;
			}
			LOGGER.info("Fetched User Info objects in range - Start : {}. Count : {}", startIndex, scanResult.getCount());
			for (UserInfo userInfo : scanResult.getResponse()) {
				this.localQueue.add(userInfo);
			}
		}
		parallelFetch.release();
		return this.localQueue.poll();
	}

	/**
	 * Interface method implementation. Reinitializes the result count
	 * @see org.springframework.batch.item.ItemStream#open(org.springframework.batch.item.ExecutionContext)
	 */
	public void open(ExecutionContext context) throws ItemStreamException {
		this.resultCount = (0 - BATCH_SIZE);
	}

	/**
	 * Interface method implementations. Does nothing
	 * @see org.springframework.batch.item.ItemStream#update(org.springframework.batch.item.ExecutionContext)
	 */
	public void update(ExecutionContext context) throws ItemStreamException {}
	public void close() throws ItemStreamException {}

}
