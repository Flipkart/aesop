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
package com.flipkart.aesop.bootstrap.mysql.txnprocessor;


/**
 * <code>TransactionProcessor>/code> defines contracts for all transaction related operations
 */
public interface TransactionProcessor
{
    /** Starts new transaction */
 	void startXtion();

    /** Ends existing transaction */
 	void endXtion(long eventTimeStamp);

    /** Has Txn already begun?  */
 	boolean isBeginTxnSeen();

    /** Begin Txn  */
 	void setBeginTxnSeen(boolean beginTxnSeen);

    /** Rest Txn */
 	void resetTxn();
}