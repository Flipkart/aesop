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
package com.flipkart.aesop.serializer.batch.processor;

import org.springframework.batch.item.ItemProcessor;

/**
 * The <code>PassThroughItemProcessor</code> class is a simple implementation of the Spring Batch {@link ItemProcessor} that acts as a pass through for the items
 * being processed.
 * 
 * @author Regunath B
 * @version 1.0, 28 Feb 2014
 */

public class PassThroughItemProcessor<T> implements ItemProcessor<T,T> {
	/**
	 *  Interface method implementation. Does nothing and acts as a simple pass-through
	 * @see org.springframework.batch.item.ItemProcessor#process(java.lang.Object)
	 */
	public T process(T item) throws Exception {
		return item;
	}
}
