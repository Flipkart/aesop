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

import com.flipkart.aesop.serializer.model.UserInfo;

/**
 * User Info service scan result object
 * @author Regunath B
 *
 */
public class ScanResult {
	int count;
	UserInfo[] response;
	public ScanResult() {		
	}
	public ScanResult(int count, UserInfo[] response) {
		super();
		this.count = count;
		this.response = response;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public UserInfo[] getResponse() {
		return response;
	}
	public void setResponse(UserInfo[] response) {
		this.response = response;
	}	
}
