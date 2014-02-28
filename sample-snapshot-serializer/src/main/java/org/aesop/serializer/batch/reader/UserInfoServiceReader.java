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
package org.aesop.serializer.batch.reader;

import org.aesop.serializer.model.UserInfo;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.trpr.platform.batch.common.BatchException;
import org.trpr.platform.batch.impl.spring.reader.CompositeItemStreamReader;
import org.trpr.platform.batch.spi.spring.reader.BatchItemStreamReader;

/**
 * The <code>UserInfoServiceReader</code> class is a simple implementation of the {@link BatchItemStreamReader} that returns the sample data item {@link UserInfo} instances
 * from JSON over REST calls to a deployed service.
 * 
 * @author Regunath B
 * @version 1.0, 29 Feb 2014
 */

public class UserInfoServiceReader <T extends UserInfo> implements BatchItemStreamReader<UserInfo> {

	/** The service endpoint URL. Note: This is for testing and very specific to this sample*/
	private static final String SERVICE_URL = "http://localhost:25151/userservice/v0.1/customer";
	
	/** The ObjectMapper to use for JSON deserialization*/
	private ObjectMapper objectMapper = new ObjectMapper();
	
	/** A set of fictitious phone numbers to perform lookup on*/
	private static final String[] PHONE_NUMBERS = {
		"6543217890",
		"6543217891",
		"9090912345",
		"9586545778",
		"9632828337",
		"9740417580",
		"9845452123",
		"9876543210",
		"9898989898",
		"9933551100",
	};
	
	/**
	 * Returns a number of {@link UserInfo} instances looked up from a service end-point.
	 * Note : The end-point and parameters used here are very specific to this sample. Also the code is mostly for testing and production 
	 * ready (no Http connection pools etc.) 
	 * @see org.trpr.platform.batch.spi.spring.reader.BatchItemStreamReader#batchRead(org.springframework.batch.item.ExecutionContext)
	 */
	public UserInfo[] batchRead(ExecutionContext context) throws Exception, UnexpectedInputException, ParseException {
		objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		UserInfo[] results = new UserInfo[PHONE_NUMBERS.length];
		for (int i =0; i < PHONE_NUMBERS.length; i++) {
			DefaultHttpClient httpclient  =  new DefaultHttpClient();
			HttpGet executionGet= new HttpGet(SERVICE_URL);
			URIBuilder uriBuilder = new URIBuilder(executionGet.getURI());
			uriBuilder.addParameter("primary_phone",PHONE_NUMBERS[i]);
			uriBuilder.addParameter("require","{\"preferences\":true,\"addresses\":true}");
			((HttpRequestBase) executionGet).setURI(uriBuilder.build());
	        HttpResponse httpResponse = httpclient.execute(executionGet);
			String response = new String(EntityUtils.toByteArray(httpResponse.getEntity()));
			SearchResult searchResult = objectMapper.readValue(response, SearchResult.class);
			results[i] = searchResult.results[0]; // we take only the first result
		}
		return results;
	}

	/**
	 * Interface method implementation. Throws an exception suggesting to use the {@link #batchRead()} method instead via the {@link CompositeItemStreamReader} instead
	 * @see org.springframework.batch.item.ItemReader#read()
	 */
	public UserInfo read() throws Exception, UnexpectedInputException, ParseException  {
		throw new BatchException("Operation is not supported! Use the CompositeItemStreamReader#read() method instead.");
	}

	/** No Op methods. These callbacks may be implemented in production systems to do stuff like Http client connection pool setup and tear down*/
	public void open(ExecutionContext context) throws ItemStreamException {
	}
	public void update(ExecutionContext context) throws ItemStreamException {
	}
	public void close() throws ItemStreamException {
	}
}
