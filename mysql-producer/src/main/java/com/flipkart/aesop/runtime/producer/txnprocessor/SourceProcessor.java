/*
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.runtime.producer.txnprocessor;

/**
 * <code>SourceProcessor>/code> defines contracts for all event source related operations
 * Example: Table name related changes
 * @author Shoury B
 * @version 1.0, 07 Mar 2014
 */
public interface SourceProcessor
{
	/** Set the source for the current set of changes */
	void setSource(long newTableId);

	/** Get current table name */
	String getCurrTableName();

	/** Get current table id */
	long getCurrTableId();
}
