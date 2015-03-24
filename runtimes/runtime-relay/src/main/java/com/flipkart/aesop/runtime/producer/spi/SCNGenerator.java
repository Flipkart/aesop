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
package com.flipkart.aesop.runtime.producer.spi;

import com.linkedin.databus2.producers.EventProducer;

/**
 * <code>SCNGenerator</code> is an abstraction for the SCN generation logic. {@link EventProducer} instances may use this interface to permit multiple implementations of SCN generation
 * logic to be plugged in. For e.g. with MySQL, an implementation of this interface may support mastership changes between a master node and a hot-standby slave and still produce monotonically
 * increasing SCNs and also preserve ordering of updates.
 *
 * @author Regunath B
 * @version 1.0, 23 Mar 2015
 */
public interface SCNGenerator {
	
	/**
	 * Returns a relay SCN using the supplied local SCN and host Id from which the local SCN was read
	 * @param localSCN the local SCN on the host
	 * @param hostId the host identifier from which the local SCN was read
	 * @return relay SCN for the local SCN
	 */
	public long getSCN(long localSCN, String hostId);

}
