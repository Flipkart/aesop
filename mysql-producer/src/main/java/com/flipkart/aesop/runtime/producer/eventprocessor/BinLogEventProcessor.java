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
package com.flipkart.aesop.runtime.producer.eventprocessor;


import com.flipkart.aesop.runtime.producer.eventlistener.OpenReplicationListener;
import com.google.code.or.binlog.BinlogEventV4;

/**
 * The <code>BinLogEventProcessor</code> is a base interface for all bin log event processors such as InsertEventProcessor,UpdateEventProcessor etc 
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public interface BinLogEventProcessor {

	/**All the event processors provide event specific processing logic in respective implementations*/
	void process(BinlogEventV4 event,OpenReplicationListener listener) throws Exception; 
}
