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
package com.flipkart.aesop.runtime.producer.txnprocessor.impl;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;

/**
 * <code>NaiveSCNGenerator</code> is a simple implementation of the {@link SCNGenerator} that returns the local SCN. 
 * Ignores the host identifier where it was generated, thereby extinguishing any possibility of being cluster aware or
 * interpreting mastership changes.
 *
 * @author Regunath B
 * @version 1.0, 24 Mar 2015
 */
public class NaiveSCNGenerator implements SCNGenerator {

	/**
	 * Returns the local SCN ignoring the host identifier.
	 * @see com.flipkart.aesop.runtime.producer.SCNGenerator#getSCN(long, java.lang.String)
	 */
	public long getSCN(long localSCN, String hostId) {
		return localSCN;
	}

}
