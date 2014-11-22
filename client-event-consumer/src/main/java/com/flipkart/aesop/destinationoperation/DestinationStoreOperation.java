/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 *******************************************************************************/

package com.flipkart.aesop.destinationoperation;

import javax.naming.OperationNotSupportedException;

import com.flipkart.aesop.event.AbstractEvent;

/**
 * Topmost class which is to be implemented by any class performing any type of Destination Store Operation.
 * @author Prakhar Jain
 */
public interface DestinationStoreOperation
{
	/**
	 * Function to be called to perform any destination store operation.
	 * @param event
	 * @throws OperationNotSupportedException
	 */
	public void execute(AbstractEvent event) throws OperationNotSupportedException;
}
